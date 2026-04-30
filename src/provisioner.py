"""
Provisioner — création des ressources OpenStack (image, flavor, keypair, instances).

Améliorations v2 :
- Utilise JumpHostClient pour SSH (plus de duplication)
- Image sélectionnée dynamiquement depuis os_info détecté par le scanner
- Flavor auto-sized : RAM/disk calculés depuis les ressources réelles des conteneurs
- ssh_user lu depuis config (plus de 'ubuntu' hardcodé)
- _is_tenant_ip() supprimé (géré par JumpHostClient)
"""

import logging
import math
import os
import time

import paramiko

from src.jump_client import JumpHostClient

logger = logging.getLogger("migration")

# Marge appliquée sur RAM et disk lors du sizing automatique
RAM_MARGIN = 1.5    # x1.5 la RAM utilisée
DISK_MARGIN = 2.0   # x2 le disk utilisé


class Provisioner:

    def __init__(self, config: dict, rollback):
        self.config = config
        self.rollback = rollback
        self.conn = None

    def set_connection(self, conn):
        self.conn = conn

    # -----------------------------------------------------------------------
    # Image
    # -----------------------------------------------------------------------

    def _resolve_image_url(self, inventory: list) -> str:
        """
        Pick the cloud image URL from the first container's OS info.
        Falls back to the config image_name.
        """
        for container in inventory:
            url = container.get("os", {}).get("image_url", "")
            if url:
                return url
        # Fallback: Ubuntu 22.04 Jammy
        return (
            "https://cloud-images.ubuntu.com/jammy/current/"
            "jammy-server-cloudimg-amd64.img"
        )

    def _resolve_image_name(self, inventory: list) -> str:
        """Image name to register in Glance."""
        for container in inventory:
            codename = container.get("os", {}).get("codename", "")
            if codename:
                return f"{container['os']['id']}-{container['os']['version']}"
        return self.config["compute"]["image_name"]

    def ensure_image(self, inventory: list):
        image_name = self._resolve_image_name(inventory)

        existing = self.conn.image.find_image(image_name)
        if existing:
            logger.info(f"Image {image_name} already exists, reusing")
            return existing

        image_url = self._resolve_image_url(inventory)
        logger.info(
            f"Image {image_name} not found — downloading to jump host..."
        )

        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_user = self.config["jump"]["username"]
        jump_password = self.config["jump"]["password"]

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect(
            hostname=jump_host, username=jump_user,
            password=jump_password, timeout=10,
            gss_auth=False, gss_kex=False,
            look_for_keys=False, allow_agent=False,
        )

        remote_path = "/tmp/migration-image.img"
        logger.info(f"Downloading {image_url} on jump host...")
        _, stdout, stderr = client.exec_command(
            f"wget -q '{image_url}' -O {remote_path}"
        )
        if stdout.channel.recv_exit_status() != 0:
            err = stderr.read().decode()
            client.close()
            raise Exception(f"Image download failed: {err}")

        logger.info("Uploading image to Glance...")
        _, stdout, stderr = client.exec_command(
            f"source ~/admin-openrc && "
            f"openstack image create \"{image_name}\" "
            f"--file {remote_path} "
            f"--disk-format qcow2 "
            f"--container-format bare --public"
        )
        if stdout.channel.recv_exit_status() != 0:
            err = stderr.read().decode()
            client.close()
            raise Exception(f"Image upload failed: {err}")

        client.exec_command(f"rm -f {remote_path}")
        client.close()

        logger.info(f"Image {image_name} uploaded to Glance")
        return self.conn.image.find_image(image_name)

    # -----------------------------------------------------------------------
    # Flavor (auto-sized)
    # -----------------------------------------------------------------------

    def ensure_flavor(self, container: dict):
        comp = self.config["compute"]
        flavor_name = comp.get("flavor_name", "").strip()

        if flavor_name:
            existing = self.conn.compute.find_flavor(flavor_name)
            if existing:
                logger.info(f"Flavor {flavor_name} already exists, reusing")
                return existing
            ram = comp.get("flavor_ram", 1024)
            vcpus = comp.get("flavor_vcpus", 1)
            disk = comp.get("flavor_disk", 10)
        else:
            # Auto-size from this container's actual resource usage
            ram_mb = container.get("ram_mb", {}).get("used", 256)
            disk_mb = container.get("disk_mb", 1024)
            ram = max(512, math.ceil(ram_mb * RAM_MARGIN / 128) * 128)
            disk = max(10, math.ceil(disk_mb * DISK_MARGIN / 1024))
            vcpus = 1
            flavor_name = f"migration-auto-{ram}mb-{disk}gb"

            existing = self.conn.compute.find_flavor(flavor_name)
            if existing:
                logger.info(
                    f"Auto-sized flavor {flavor_name} already exists, reusing"
                )
                return existing

        flavor = self.conn.compute.create_flavor(
            name=flavor_name,
            ram=ram,
            vcpus=vcpus,
            disk=disk
        )
        logger.info(
            f"Created flavor: {flavor_name} "
            f"(RAM={ram}MB, vCPUs={vcpus}, Disk={disk}GB)"
        )
        return flavor

    # -----------------------------------------------------------------------
    # Keypair
    # -----------------------------------------------------------------------

    def ensure_keypair(self):
        comp = self.config["compute"]
        name = comp["keypair_name"]
        key_dir = self.config["paths"]["key_dir"]
        private_key_path = os.path.join(key_dir, name)

        existing = self.conn.compute.find_keypair(name)
        if existing:
            if os.path.exists(private_key_path):
                logger.info(f"Keypair {name} already exists, reusing")
                return existing, private_key_path
            else:
                logger.warning(
                    f"Keypair {name} exists in Nova but private key missing "
                    f"locally — recreating"
                )
                self.conn.compute.delete_keypair(name)

        if not os.path.exists(key_dir):
            os.makedirs(key_dir)

        key = paramiko.RSAKey.generate(2048)
        key.write_private_key_file(private_key_path)
        os.chmod(private_key_path, 0o600)

        keypair = self.conn.compute.create_keypair(
            name=name,
            public_key=f"ssh-rsa {key.get_base64()}"
        )
        logger.info(f"Created keypair: {name} → {private_key_path}")
        return keypair, private_key_path

    # -----------------------------------------------------------------------
    # Cinder volume
    # -----------------------------------------------------------------------

    def ensure_volume(self, name: str, size: int):
        existing = self.conn.block_storage.find_volume(name)
        if existing:
            if existing.status in ("available", "in-use"):
                logger.info(f"Volume {name} already exists, reusing")
                return existing
            else:
                logger.warning(
                    f"Volume {name} status={existing.status} — recreating"
                )
                self.conn.block_storage.delete_volume(existing.id)
                self.conn.block_storage.wait_for_delete(existing)

        volume = self.conn.block_storage.create_volume(name=name, size=size)
        self.conn.block_storage.wait_for_status(
            volume, status="available", wait=120
        )
        self.rollback.register("volume", volume.id, name)
        logger.info(f"Volume {name} created ({size}GB)")
        return volume

    def attach_volume(self, server_id: str, volume_id: str,
                      server_name: str):
        for att in self.conn.compute.volume_attachments(server_id):
            if att.volume_id == volume_id:
                logger.info(f"Volume already attached to {server_name}")
                return att
        att = self.conn.compute.create_volume_attachment(
            server_id, volume_id=volume_id
        )
        logger.info(f"Volume attached to {server_name}")
        return att

    # -----------------------------------------------------------------------
    # Instance
    # -----------------------------------------------------------------------

    def create_instance(self, name: str, image, flavor, port,
                        keypair_name: str):
        existing = self.conn.compute.find_server(name)
        if existing:
            if existing.status == "ACTIVE":
                logger.info(
                    f"Instance {name} already ACTIVE, reusing"
                )
                return existing
            else:
                logger.warning(
                    f"Instance {name} status={existing.status} — recreating"
                )
                self.conn.compute.delete_server(existing.id)
                self.conn.compute.wait_for_delete(existing)

        # Cloud-init userdata: disable UseDNS in sshd (reverse DNS lookup
        # on the client IP can add 30s to every SSH connection when there is
        # no PTR record — which is always the case on a tenant network).
        userdata = (
            "#!/bin/bash\n"
            "sed -i 's/^#*UseDNS.*/UseDNS no/' /etc/ssh/sshd_config\n"
            "systemctl restart sshd\n"
        )
        server = self.conn.compute.create_server(
            name=name,
            image_id=image.id,
            flavor_id=flavor.id,
            networks=[{"port": port.id}],
            key_name=keypair_name,
            config_drive=True,
            user_data=userdata,
        )
        self.rollback.register("server", server.id, name)
        logger.info(f"Creating instance: {name}")

        server = self.conn.compute.wait_for_server(
            server, status="ACTIVE", wait=180
        )
        logger.info(f"Instance {name} is ACTIVE")
        return server

    # -----------------------------------------------------------------------
    # Main orchestration
    # -----------------------------------------------------------------------

    def provision_all(self, inventory: list, ports: dict):
        image = self.ensure_image(inventory)
        keypair, private_key_path = self.ensure_keypair()
        keypair_name = self.config["compute"]["keypair_name"]

        # Cinder volume for MariaDB
        storage_config = self.config.get("storage", {})
        volume = None
        if storage_config:
            volume = self.ensure_volume(
                storage_config["mariadb_volume_name"],
                storage_config["mariadb_volume_size"]
            )

        instances = {}
        for container in inventory:
            name = container["name"]
            if name not in ports:
                continue
            flavor = self.ensure_flavor(container)
            server = self.create_instance(
                f"instance-{name}",
                image, flavor,
                ports[name],
                keypair_name
            )
            instances[name] = server

        # Attach Cinder volume to MariaDB instance
        if volume:
            for container in inventory:
                if container["service"] == "mariadb":
                    name = container["name"]
                    if name in instances:
                        self.attach_volume(
                            instances[name].id,
                            volume.id,
                            f"instance-{name}"
                        )
                    break

        # Wait for SSH on all instances
        # cfg_ssh_user is read once before the loop so mutations don't bleed
        # across containers with different OSes (ubuntu vs debian).
        cfg_ssh_user = self.config.get("compute", {}).get("ssh_user", "").strip()
        for container in inventory:
            name = container["name"]
            if name not in ports:
                continue

            detected = container.get("os", {}).get("default_ssh_user", "ubuntu")
            self.config["compute"]["ssh_user"] = cfg_ssh_user or detected

            ip = ports[name].fixed_ips[0]["ip_address"]
            with JumpHostClient(self.config) as jc:
                ssh_ready = jc.wait_for_ssh(ip, private_key_path, timeout=300)
            if not ssh_ready:
                logger.warning(
                    f"Cannot reach {name} via SSH at {ip} — "
                    f"continuing anyway for diagnostics"
                )

        logger.info(
            f"Provisioning complete: {len(instances)} instances created"
        )
        return instances, private_key_path
