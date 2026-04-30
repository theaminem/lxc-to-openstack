"""
Restorer — installation et restauration des services sur les instances OpenStack.

Améliorations v2 :
- Utilise JumpHostClient (plus de duplication SSH)
- Credentials MariaDB passés en paramètre (plus de hardcode 'password')
- _is_tenant_ip() supprimé (géré par JumpHostClient)
- bind-address sur l'IP de l'instance, pas 0.0.0.0
- Chemin config MariaDB détecté dynamiquement via le scanner
- Provider interface et CIDR lus depuis config (plus de 'ens35' hardcodé)
- Proxy APT optionnel (use_proxy: false = accès Internet direct)
- DROP USER dynamique basé sur le subnet source réel
"""

import logging
import shlex
import subprocess

import paramiko

from src.jump_client import JumpHostClient

logger = logging.getLogger("migration")


class Restorer:

    def __init__(self, config: dict):
        self.config = config

    # -----------------------------------------------------------------------
    # APT proxy
    # -----------------------------------------------------------------------

    def _ensure_apt_proxy_on_jump(self):
        """Install and start apt-cacher-ng on the jump host (called once)."""
        apt_cfg = self.config.get("apt", {})
        if not apt_cfg.get("use_proxy", True):
            return

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

        # Skip install if already installed and running
        _, st, _ = client.exec_command(
            "systemctl is-active apt-cacher-ng 2>/dev/null"
        )
        already = st.channel.recv_exit_status() == 0
        if already:
            client.close()
            logger.info(
                f"APT proxy already running on {jump_host}:"
                f"{apt_cfg.get('proxy_port', 3142)}"
            )
            return

        # sudo with password piped via stdin
        escaped_pwd = jump_password.replace("'", "'\"'\"'")
        sudo_prefix = f"echo '{escaped_pwd}' | sudo -S -p '' "
        cmd = (
            f"{sudo_prefix}DEBIAN_FRONTEND=noninteractive "
            f"apt-get install -y apt-cacher-ng && "
            f"{sudo_prefix}systemctl enable apt-cacher-ng && "
            f"{sudo_prefix}systemctl start apt-cacher-ng"
        )
        _, stdout, stderr = client.exec_command(cmd)
        rc = stdout.channel.recv_exit_status()
        client.close()
        if rc != 0:
            raise Exception(
                f"apt-cacher-ng setup failed: {stderr.read().decode().strip()}"
            )
        logger.info(
            f"APT proxy ready on {jump_host}:{apt_cfg.get('proxy_port', 3142)}"
        )

    def _configure_apt_proxy_on_instance(self, jc: JumpHostClient, client):
        apt_cfg = self.config.get("apt", {})
        if not apt_cfg.get("use_proxy", True):
            # Make sure no leftover proxy config from a previous run
            jc.run_soft(
                client,
                "sudo rm -f /etc/apt/apt.conf.d/00proxy",
                "Removing any leftover APT proxy"
            )
            logger.info("  APT proxy disabled — direct Internet access")
            return
        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        proxy_port = apt_cfg.get("proxy_port", 3142)
        jc.run(
            client,
            f"echo 'Acquire::http::Proxy \"http://{jump_host}:{proxy_port}\";' "
            f"| sudo tee /etc/apt/apt.conf.d/00proxy > /dev/null",
            f"Setting APT proxy to {jump_host}:{proxy_port}"
        )

    def _fix_apt_sources(self, jc: JumpHostClient, client,
                          codename: str = ""):
        if not codename:
            codename = jc.run(
                client, "lsb_release -cs", "Detecting OS release"
            ).strip() or "jammy"

        # Use HTTPS by default — protects against transparent HTTP proxies
        # / port 80 interception by network gear or hypervisor services.
        # Disable with: apt.use_https: false in config.yml
        scheme = "https" if self.config.get("apt", {}).get("use_https", True) else "http"

        jc.run_soft(
            client,
            "sudo rm -f /etc/apt/sources.list.d/ubuntu.sources",
            "Removing cloud APT sources"
        )
        jc.run(
            client,
            f"printf '%s\\n' "
            f"'deb {scheme}://archive.ubuntu.com/ubuntu {codename} main restricted universe multiverse' "
            f"'deb {scheme}://archive.ubuntu.com/ubuntu {codename}-updates main restricted universe multiverse' "
            f"'deb {scheme}://security.ubuntu.com/ubuntu {codename}-security main restricted universe multiverse' "
            f"| sudo tee /etc/apt/sources.list > /dev/null",
            f"Writing APT sources ({codename}, {scheme})"
        )
        jc.run(client, "sudo apt-get clean", "Cleaning APT cache")
        jc.run(
            client,
            "sudo DEBIAN_FRONTEND=noninteractive apt-get update",
            "Updating APT index"
        )

    def _apt_install(self, jc: JumpHostClient, client, packages: str,
                     description: str = ""):
        """apt-get install with one automatic retry on 404 (stale mirror index)."""
        cmd = f"sudo DEBIAN_FRONTEND=noninteractive apt-get install -y {packages}"
        try:
            jc.run(client, cmd, description)
        except Exception as first_err:
            if "404" not in str(first_err) and "Unable to fetch" not in str(first_err):
                raise
            logger.warning("apt-get install hit a 404 — refreshing APT index and retrying...")
            jc.run(client, "sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq",
                   "Refreshing APT index (retry)")
            jc.run(client, cmd, f"{description} (retry)")

    # -----------------------------------------------------------------------
    # IP / hosts mapping
    # -----------------------------------------------------------------------

    def _inject_hosts_mapping(self, jc: JumpHostClient, client,
                               host_map: dict):
        if not host_map:
            return
        jc.run_soft(
            client,
            "sudo cp /etc/hosts /etc/hosts.backup.$(date +%s)",
            "Backing up /etc/hosts"
        )
        for name, ip in host_map.items():
            jc.run_soft(
                client,
                f"sudo sed -i '/ {name}\\.internal$/d' /etc/hosts"
            )
            jc.run(
                client,
                f"echo '{ip} {name}.internal' "
                f"| sudo tee -a /etc/hosts > /dev/null",
                f"Adding {name}.internal -> {ip}"
            )

    def _replace_ip_in_configs(self, jc: JumpHostClient, client,
                                old_ip: str, new_value: str):
        # Escape dots so sed treats them as literals, not regex wildcards
        escaped_old = old_ip.replace(".", "\\.")
        safe_paths = "/etc /var/www /root"
        jc.run_soft(
            client,
            f"sudo grep -rl '{escaped_old}' {safe_paths} 2>/dev/null | "
            f"while read f; do "
            f"sudo sed -i 's/{escaped_old}/{new_value}/g' \"$f\"; "
            f"done",
            f"Replacing {old_ip} -> {new_value} in configs"
        )

    # -----------------------------------------------------------------------
    # MariaDB
    # -----------------------------------------------------------------------

    def restore_mariadb(self, ip: str, private_key_path: str,
                        container: dict, db_password: str):
        logger.info(f"Restoring MariaDB on {ip}...")
        os_info = container.get("os", {})
        codename = os_info.get("codename", "jammy")
        mariadb_cnf = os_info.get(
            "mariadb_cnf_path",
            "/etc/mysql/mariadb.conf.d/50-server.cnf"
        )
        app_users = container.get("app_users", [])

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            self._configure_apt_proxy_on_instance(jc, client)
            self._fix_apt_sources(jc, client, codename)

            self._apt_install(jc, client, "mariadb-server", "Installing mariadb-server")

            # Cinder volume setup
            vdb_check = jc.run_soft(client, "ls /dev/vdb 2>/dev/null")
            if vdb_check:
                logger.info("  Cinder volume detected, configuring...")
                jc.run_soft(client, "sudo systemctl stop mariadb",
                            "Stopping MariaDB")
                already_formatted = jc.run_soft(
                    client, "sudo blkid /dev/vdb", "Checking volume format"
                )
                if not already_formatted:
                    jc.run(client, "sudo mkfs.ext4 /dev/vdb",
                           "Formatting Cinder volume")
                else:
                    logger.info("  Volume already formatted, skipping mkfs")
                jc.run(client, "sudo mkdir -p /mnt/mariadb-data",
                       "Creating mount point")
                jc.run(client, "sudo mount /dev/vdb /mnt/mariadb-data",
                       "Mounting temporarily")
                jc.run(client,
                       "sudo cp -a /var/lib/mysql/. /mnt/mariadb-data/",
                       "Copying data to volume")
                jc.run(client, "sudo umount /mnt/mariadb-data")
                jc.run(client, "sudo mount /dev/vdb /var/lib/mysql",
                       "Mounting to /var/lib/mysql")
                jc.run(client, "sudo chown -R mysql:mysql /var/lib/mysql",
                       "Fixing permissions")
                jc.run_soft(
                    client,
                    "sudo sed -i '\\|/dev/vdb /var/lib/mysql|d' /etc/fstab"
                )
                jc.run(
                    client,
                    "echo '/dev/vdb /var/lib/mysql ext4 defaults 0 2' "
                    "| sudo tee -a /etc/fstab > /dev/null",
                    "Persisting mount in fstab"
                )
            else:
                logger.warning("  No Cinder volume — using instance disk")

            jc.run_soft(client, "sudo systemctl start mariadb",
                        "Starting MariaDB")
            jc.run(
                client,
                "sudo mysql -u root < /tmp/mariadb_dump.sql",
                "Importing database dump"
            )

            # Recreate app users with runtime password (no hardcode)
            bridge_subnet = self.config.get("source", {}).get(
                "bridge_subnet", ""
            )
            subnet_prefix = (
                ".".join(bridge_subnet.split(".")[:3])
                if bridge_subnet else ""
            )
            if app_users:
                for u in app_users:
                    user = u["user"]
                    safe_user = user.replace("'", "\\'")
                    safe_pwd = db_password.replace("'", "\\'")
                    create_sql = (
                        f"CREATE USER IF NOT EXISTS '{safe_user}'@'%' "
                        f"IDENTIFIED BY '{safe_pwd}'; "
                        f"GRANT ALL PRIVILEGES ON *.* TO '{safe_user}'@'%'; "
                        f"FLUSH PRIVILEGES;"
                    )
                    jc.run(
                        client,
                        f"echo {shlex.quote(create_sql)} | sudo mysql -u root",
                        f"Recreating user '{user}'"
                    )
                    if subnet_prefix:
                        del_sql = (
                            f"DELETE FROM mysql.user "
                            f"WHERE User='{safe_user}' "
                            f"AND Host LIKE '{subnet_prefix}.%'; "
                            f"FLUSH PRIVILEGES;"
                        )
                        jc.run_soft(
                            client,
                            f"echo {shlex.quote(del_sql)} | sudo mysql -u root"
                        )
            else:
                logger.warning("  No app users in inventory — skipping")

            # Bind to instance IP only (not 0.0.0.0)
            jc.run(
                client,
                f"sudo sed -i 's/^bind-address.*/bind-address = {ip}/' "
                f"{mariadb_cnf}",
                f"Setting bind-address to {ip}"
            )
            jc.run_soft(client, "sudo systemctl restart mariadb",
                        "Restarting MariaDB")

        logger.info("MariaDB restoration complete")

    # -----------------------------------------------------------------------
    # Apache
    # -----------------------------------------------------------------------

    def restore_apache(self, ip: str, private_key_path: str,
                       container: dict):
        logger.info(f"Restoring Apache on {ip}...")
        codename = container.get("os", {}).get("codename", "jammy")

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            self._configure_apt_proxy_on_instance(jc, client)
            self._fix_apt_sources(jc, client, codename)

            self._apt_install(jc, client,
                              "apache2 php php-mysql libapache2-mod-php",
                              "Installing Apache + PHP")
            jc.run(
                client,
                "sudo tar xzf /tmp/apache_backup.tar.gz -C /",
                "Extracting Apache backup"
            )
            jc.run_soft(client, "sudo rm -f /var/www/html/index.html",
                        "Removing default page")
            jc.run_soft(
                client,
                "sudo a2enmod ssl rewrite headers 2>/dev/null || true",
                "Enabling common modules"
            )
            modules_output = jc.run_soft(
                client, "cat /tmp/apache_modules.txt"
            )
            for line in modules_output.split("\n"):
                line = line.strip()
                if "_module" in line and "(" in line:
                    mod = line.split("_module")[0].strip()
                    jc.run_soft(
                        client,
                        f"sudo a2enmod {mod} 2>/dev/null || true"
                    )
            jc.run_soft(
                client,
                "sudo a2ensite *.conf 2>/dev/null || true",
                "Enabling virtual hosts"
            )
            jc.run(client, "sudo systemctl restart apache2",
                   "Restarting Apache")

        logger.info("Apache restoration complete")

    # -----------------------------------------------------------------------
    # Backup service
    # -----------------------------------------------------------------------

    def restore_backup(self, ip: str, private_key_path: str,
                       container: dict):
        logger.info(f"Restoring Backup service on {ip}...")
        codename = container.get("os", {}).get("codename", "jammy")

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            self._configure_apt_proxy_on_instance(jc, client)
            self._fix_apt_sources(jc, client, codename)

            self._apt_install(jc, client, "mariadb-client", "Installing mariadb-client")
            jc.run_soft(
                client,
                "sudo cp /tmp/backup_script.sh /root/backup.sh "
                "&& sudo chmod +x /root/backup.sh",
                "Installing backup script"
            )
            jc.run_soft(
                client, "sudo crontab /tmp/backup_crontab.txt",
                "Installing crontab"
            )

        logger.info("Backup service restoration complete")

    # -----------------------------------------------------------------------
    # NFS
    # -----------------------------------------------------------------------

    def restore_nfs(self, ip: str, private_key_path: str,
                    container: dict, shared_dirs: list):
        logger.info(f"Restoring NFS on {ip}...")
        codename = container.get("os", {}).get("codename", "jammy")

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            self._configure_apt_proxy_on_instance(jc, client)
            self._fix_apt_sources(jc, client, codename)

            self._apt_install(jc, client, "nfs-kernel-server", "Installing NFS server")
            jc.run(client, "sudo cp /tmp/nfs_exports.txt /etc/exports",
                   "Restoring /etc/exports")
            for d in shared_dirs:
                jc.run(client, f"sudo mkdir -p {d}",
                       f"Creating shared dir {d}")
            jc.run_soft(
                client, "sudo tar xzf /tmp/nfs_data.tar.gz -C /",
                "Extracting NFS data"
            )
            jc.run(client, "sudo exportfs -ra", "Applying NFS exports")
            jc.run(client, "sudo systemctl restart nfs-kernel-server",
                   "Restarting NFS")

        logger.info("NFS restoration complete")

    # -----------------------------------------------------------------------
    # FTP
    # -----------------------------------------------------------------------

    def restore_ftp(self, ip: str, private_key_path: str,
                    container: dict, server_type: str):
        logger.info(f"Restoring FTP ({server_type}) on {ip}...")
        codename = container.get("os", {}).get("codename", "jammy")

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            self._configure_apt_proxy_on_instance(jc, client)
            self._fix_apt_sources(jc, client, codename)

            self._apt_install(jc, client, server_type, f"Installing {server_type}")
            config_dest = (
                "/etc/vsftpd.conf"
                if server_type == "vsftpd"
                else "/etc/proftpd/proftpd.conf"
            )
            if server_type == "proftpd":
                jc.run(client, "sudo mkdir -p /etc/proftpd")

            jc.run(
                client,
                f"sudo cp /tmp/ftp_config.conf {config_dest}",
                "Restoring FTP config"
            )
            users_output = jc.run_soft(
                client, "cat /tmp/ftp_users.txt", "Reading FTP users"
            )
            for line in users_output.split("\n"):
                parts = line.split(":")
                if len(parts) >= 3:
                    try:
                        uid = int(parts[2])
                        username = parts[0]
                        if uid >= 1000 and username not in ("nobody",):
                            jc.run_soft(
                                client,
                                f"sudo useradd -m {username} 2>/dev/null || true"
                            )
                    except ValueError:
                        pass

            jc.run_soft(
                client, "sudo tar xzf /tmp/ftp_data.tar.gz -C /",
                "Extracting FTP data"
            )
            jc.run(client, f"sudo systemctl restart {server_type}",
                   f"Restarting {server_type}")

        logger.info("FTP restoration complete")

    # -----------------------------------------------------------------------
    # IP remapping
    # -----------------------------------------------------------------------

    def update_ip_mappings(self, ip: str, private_key_path: str,
                           ip_map: dict, service_host_map: dict):
        logger.info(f"Updating IP mappings on {ip}...")

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)

            if service_host_map:
                self._inject_hosts_mapping(jc, client, service_host_map)

            for old_ip, new_ip in ip_map.items():
                self._replace_ip_in_configs(jc, client, old_ip, new_ip)

            for svc in ("apache2", "mariadb", "nfs-kernel-server",
                        "vsftpd", "proftpd"):
                jc.run_soft(
                    client,
                    f"sudo systemctl restart {svc} 2>/dev/null || true"
                )

        logger.info("IP mappings updated")

    # -----------------------------------------------------------------------
    # NAT on migration host
    # -----------------------------------------------------------------------

    def _setup_nat(self):
        os_cfg = self.config.get("openstack", {})
        interface = os_cfg.get("provider_interface", "ens35")
        cidr = os_cfg.get("provider_cidr", "10.0.0.0/24")

        subprocess.run(
            ["sudo", "sysctl", "-w", "net.ipv4.ip_forward=1"],
            capture_output=True
        )
        check = subprocess.run(
            ["sudo", "iptables", "-t", "nat", "-C", "POSTROUTING",
             "-s", cidr, "-o", interface, "-j", "MASQUERADE"],
            capture_output=True
        )
        if check.returncode != 0:
            subprocess.run(
                ["sudo", "iptables", "-t", "nat", "-A", "POSTROUTING",
                 "-s", cidr, "-o", interface, "-j", "MASQUERADE"],
                capture_output=True
            )
        logger.info(f"NAT routing enabled ({cidr} via {interface})")

    # -----------------------------------------------------------------------
    # Main orchestration
    # -----------------------------------------------------------------------

    def restore_all(self, inventory: list, backup_paths: dict,
                    private_key_path: str, ports: dict,
                    network_mode: str = "provider",
                    db_password: str = ""):
        self._ensure_apt_proxy_on_jump()

        ip_map: dict = {}
        service_host_map: dict = {}
        for container in inventory:
            name = container["name"]
            if name in ports:
                old_ip = container["ip"]
                new_ip = ports[name].fixed_ips[0]["ip_address"]
                service_host_map[name] = new_ip
                if old_ip != new_ip:
                    ip_map[old_ip] = new_ip

        self._setup_nat()

        for container in inventory:
            name = container["name"]
            service = container["service"]
            if name not in ports:
                continue
            ip = ports[name].fixed_ips[0]["ip_address"]

            if service == "mariadb":
                self.restore_mariadb(
                    ip, private_key_path, container, db_password
                )
            elif service == "apache":
                self.restore_apache(ip, private_key_path, container)
            elif service == "backup":
                self.restore_backup(ip, private_key_path, container)
            elif service == "nfs":
                shared_dirs = backup_paths.get(name, {}).get(
                    "shared_dirs", []
                )
                self.restore_nfs(
                    ip, private_key_path, container, shared_dirs
                )
            elif service == "ftp":
                server_type = backup_paths.get(name, {}).get(
                    "server_type", "vsftpd"
                )
                self.restore_ftp(
                    ip, private_key_path, container, server_type
                )

        if network_mode == "provider" and ip_map:
            logger.info("Provider mode: remapping IPs in configs...")
            for container in inventory:
                name = container["name"]
                if name not in ports:
                    continue
                ip = ports[name].fixed_ips[0]["ip_address"]
                self.update_ip_mappings(
                    ip, private_key_path, ip_map, service_host_map
                )
        elif network_mode == "tenant":
            logger.info(
                "Tenant mode: IPs preserved — injecting /etc/hosts only"
            )
            for container in inventory:
                name = container["name"]
                if name not in ports:
                    continue
                ip = ports[name].fixed_ips[0]["ip_address"]
                self.update_ip_mappings(
                    ip, private_key_path, {}, service_host_map
                )
        else:
            logger.warning(
                f"Unknown network mode '{network_mode}', skipping remapping"
            )

        logger.info("All restorations complete")
