import logging
import time
import os
import paramiko

logger = logging.getLogger("migration")


class Provisioner:

    def __init__(self, config, rollback):
        self.config = config
        self.rollback = rollback
        self.conn = None

    def set_connection(self, conn):
        self.conn = conn

    def ensure_image(self):
        comp = self.config["compute"]
        name = comp["image_name"]

        existing = self.conn.image.find_image(name)
        if existing:
            logger.info(f"Image {name} already exists, reusing")
            return existing

        logger.info(f"Image {name} not found, uploading on vm-cible...")

        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_user = self.config["jump"]["username"]
        jump_password = self.config["jump"]["password"]

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=jump_host,
            username=jump_user,
            password=jump_password,
            timeout=10
        )

        image_url = "https://cloud-images.ubuntu.com/jammy/current/jammy-server-cloudimg-amd64.img"
        remote_path = "/tmp/ubuntu-22.04.img"

        logger.info("Downloading Jammy image on vm-cible...")
        stdin, stdout, stderr = client.exec_command(
            f"wget -q {image_url} -O {remote_path}"
        )
        exit_code = stdout.channel.recv_exit_status()

        if exit_code != 0:
            errors = stderr.read().decode()
            client.close()
            raise Exception(f"Image download failed: {errors}")

        logger.info("Image downloaded on vm-cible")

        logger.info("Uploading image to Glance...")
        stdin, stdout, stderr = client.exec_command(
            f"source ~/admin-openrc && "
            f"openstack image create \"{name}\" "
            f"--file {remote_path} "
            f"--disk-format qcow2 "
            f"--container-format bare --public"
        )

        exit_code = stdout.channel.recv_exit_status()
        errors = stderr.read().decode()

        if exit_code != 0:
            client.close()
            raise Exception(f"Image upload failed: {errors}")

        logger.info(f"Image {name} uploaded to Glance")

        client.exec_command(f"rm -f {remote_path}")
        client.close()

        image = self.conn.image.find_image(name)
        return image

    def ensure_flavor(self):
        comp = self.config["compute"]
        name = comp["flavor_name"]

        existing = self.conn.compute.find_flavor(name)
        if existing:
            logger.info(f"Flavor {name} already exists, reusing")
            return existing

        flavor = self.conn.compute.create_flavor(
            name=name,
            ram=comp["flavor_ram"],
            vcpus=comp["flavor_vcpus"],
            disk=comp["flavor_disk"]
        )

        logger.info(
            f"Created flavor: {name} "
            f"(RAM={comp['flavor_ram']}MB, "
            f"vCPUs={comp['flavor_vcpus']}, "
            f"Disk={comp['flavor_disk']}GB)"
        )

        return flavor

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
                    f"Keypair {name} exists in Nova but private key not found locally. Recreating."
                )
                self.conn.compute.delete_keypair(name)

        if not os.path.exists(key_dir):
            os.makedirs(key_dir)

        key = paramiko.RSAKey.generate(2048)
        key.write_private_key_file(private_key_path)
        os.chmod(private_key_path, 0o600)

        public_key = f"ssh-rsa {key.get_base64()}"

        keypair = self.conn.compute.create_keypair(
            name=name,
            public_key=public_key
        )

        logger.info(f"Created keypair: {name}")
        logger.info(f"Private key saved: {private_key_path}")

        return keypair, private_key_path

    def ensure_volume(self, name, size):
        existing = self.conn.block_storage.find_volume(name)

        if existing:
            if existing.status in ["available", "in-use"]:
                logger.info(f"Volume {name} already exists, reusing")
                return existing
            else:
                logger.warning(
                    f"Volume {name} exists but status {existing.status}. Recreating."
                )
                self.conn.block_storage.delete_volume(existing.id)
                self.conn.block_storage.wait_for_delete(existing)

        logger.info(f"Creating volume {name} ({size}GB)...")

        volume = self.conn.block_storage.create_volume(
            name=name,
            size=size
        )

        self.conn.block_storage.wait_for_status(
            volume,
            status="available",
            wait=120
        )

        self.rollback.register("volume", volume.id, name)

        logger.info(f"Volume {name} created ({size}GB)")

        return volume

    def attach_volume(self, server_id, volume_id, server_name):
        attachments = list(
            self.conn.compute.volume_attachments(server_id)
        )

        for att in attachments:
            if att.volume_id == volume_id:
                logger.info(f"Volume already attached to {server_name}")
                return att

        attachment = self.conn.compute.create_volume_attachment(
            server_id,
            volume_id=volume_id
        )

        logger.info(f"Volume attached to {server_name}")

        return attachment

    def create_instance(self, name, image, flavor, port, keypair_name):
        existing = self.conn.compute.find_server(name)

        if existing:
            if existing.status == "ACTIVE":
                logger.info(f"Instance {name} already exists and ACTIVE, reusing")
                return existing
            else:
                logger.warning(f"Instance {name} exists but not ACTIVE. Recreating.")
                self.conn.compute.delete_server(existing.id)
                self.conn.compute.wait_for_delete(existing)

        server = self.conn.compute.create_server(
            name=name,
            image_id=image.id,
            flavor_id=flavor.id,
            networks=[{"port": port.id}],
            key_name=keypair_name
        )

        self.rollback.register("server", server.id, name)

        logger.info(f"Creating instance: {name}")

        server = self.conn.compute.wait_for_server(
            server,
            status="ACTIVE",
            wait=180
        )

        logger.info(f"Instance {name} is ACTIVE")

        return server

    def _is_tenant_ip(self, ip):
        source_subnet = self.config.get("source", {}).get("bridge_subnet", "")
        if source_subnet.startswith("10.0.3."):
            return ip.startswith("10.0.3.")
        return ip.startswith("10.0.3.")

    def _copy_key_to_jump_host(self, jump_client, private_key_path):
        remote_key_path = "/tmp/migration-key"

        sftp = jump_client.open_sftp()
        sftp.put(private_key_path, remote_key_path)
        sftp.close()

        stdin, stdout, stderr = jump_client.exec_command(
            f"chmod 600 {remote_key_path}"
        )
        stdout.channel.recv_exit_status()

        return remote_key_path

    def _get_router_namespace(self, jump_client):
        stdin, stdout, stderr = jump_client.exec_command(
            "ip netns | awk '/qrouter/ {print $1; exit}'"
        )
        namespace = stdout.read().decode().strip()

        if not namespace:
            raise Exception("No qrouter namespace found on vm-cible")

        return namespace

    def _wait_for_ssh_via_router_namespace(
        self,
        jump_client,
        ip,
        private_key_path
    ):
        remote_key_path = self._copy_key_to_jump_host(
            jump_client,
            private_key_path
        )

        namespace = self._get_router_namespace(jump_client)
        
        escaped_password = self.config["jump"]["password"].replace("'", "'\"'\"'")

        command = (
            f"echo '{escaped_password}' | sudo -S "
            f"ip netns exec {namespace} "
            f"ssh -o StrictHostKeyChecking=no "
            f"-o UserKnownHostsFile=/dev/null "
            f"-o ConnectTimeout=5 "
            f"-i {remote_key_path} "
            f"ubuntu@{ip} hostname"
        )

        stdin, stdout, stderr = jump_client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()

        output = stdout.read().decode().strip()
        errors = stderr.read().decode().strip()

        if exit_code == 0:
            logger.debug(f"Tenant SSH output for {ip}: {output}")
            logger.info(f"SSH ready on {ip} via {namespace}")
            return True

        if errors:
            logger.debug(f"Tenant SSH error for {ip}: {errors}")

        return False

    def _wait_for_ssh_via_direct_tunnel(
        self,
        jump_client,
        ip,
        private_key_path
    ):
        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_transport = jump_client.get_transport()

        jump_channel = jump_transport.open_channel(
            "direct-tcpip",
            (ip, 22),
            (jump_host, 0)
        )

        target_client = paramiko.SSHClient()
        target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        key = paramiko.RSAKey.from_private_key_file(private_key_path)

        target_client.connect(
            hostname=ip,
            username="ubuntu",
            pkey=key,
            sock=jump_channel,
            timeout=5
        )

        target_client.close()
        logger.info(f"SSH ready on {ip}")
        return True

    def wait_for_ssh(self, ip, private_key_path, timeout=300):
        logger.info(f"Waiting for SSH on {ip} via jump host...")

        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_user = self.config["jump"]["username"]
        jump_password = self.config["jump"]["password"]

        start = time.time()

        while time.time() - start < timeout:
            jump_client = None

            try:
                jump_client = paramiko.SSHClient()
                jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                jump_client.connect(
                    hostname=jump_host,
                    username=jump_user,
                    password=jump_password,
                    timeout=5
                )

                if self._is_tenant_ip(ip):
                    ssh_ready = self._wait_for_ssh_via_router_namespace(
                        jump_client,
                        ip,
                        private_key_path
                    )
                else:
                    ssh_ready = self._wait_for_ssh_via_direct_tunnel(
                        jump_client,
                        ip,
                        private_key_path
                    )

                if ssh_ready:
                    jump_client.close()
                    return True

            except Exception as exc:
                logger.debug(f"SSH wait retry for {ip}: {exc}")

            finally:
                if jump_client:
                    jump_client.close()

            time.sleep(5)

        logger.error(f"SSH timeout on {ip}")
        return False

    def provision_all(self, inventory, ports):
        image = self.ensure_image()
        flavor = self.ensure_flavor()
        keypair, private_key_path = self.ensure_keypair()

        keypair_name = self.config["compute"]["keypair_name"]

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

            instance_name = f"instance-{name}"
            port = ports[name]

            server = self.create_instance(
                instance_name,
                image,
                flavor,
                port,
                keypair_name
            )

            instances[name] = server

        if volume:
            for container in inventory:
                if container["service"] == "mariadb":
                    name = container["name"]

                    if name in instances:
                        server = instances[name]

                        self.attach_volume(
                            server.id,
                            volume.id,
                            f"instance-{name}"
                        )

                    break

        for container in inventory:
            name = container["name"]

            if name not in ports:
                continue

            port = ports[name]
            target_ip = port.fixed_ips[0]["ip_address"]

            ssh_ready = self.wait_for_ssh(target_ip, private_key_path)
            if not ssh_ready:
                logger.warning(
                    f"Cannot reach {name} via SSH at {target_ip}, "
                    f"continuing anyway for diagnostics"
                )

        logger.info(
            f"Provisioning complete: {len(instances)} instances"
        )

        return instances, private_key_path
