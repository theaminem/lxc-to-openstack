import logging
import subprocess
import paramiko


logger = logging.getLogger("migration")


class Restorer:

    def __init__(self, config):
        self.config = config

    def _get_ssh_client(self, ip, private_key_path):
        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_user = self.config["jump"]["username"]
        jump_password = self.config["jump"]["password"]

        jump_client = paramiko.SSHClient()
        jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        jump_client.connect(
            hostname=jump_host,
            username=jump_user,
            password=jump_password,
            timeout=10
        )

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
            timeout=10
        )

        return target_client, jump_client

    def _run_remote(self, client, command, description=""):
        if description:
            logger.info(f"  -> {description}")

        logger.debug(f"  CMD: {command}")

        stdin, stdout, stderr = client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()

        output = stdout.read().decode().strip()
        errors = stderr.read().decode().strip()

        if exit_code != 0:
            logger.error(f"  Command failed (exit {exit_code}): {command}")
            if errors:
                logger.error(f"  STDERR: {errors}")
            raise Exception(f"Remote command failed: {command}")

        if output:
            logger.debug(f"  OUTPUT: {output[:300]}")

        return output

    def _run_remote_soft(self, client, command, description=""):
        try:
            return self._run_remote(client, command, description)
        except Exception as exc:
            logger.warning(f"  Soft command failed: {description or command}")
            logger.debug(str(exc))
            return ""

    def _fix_apt_sources(self, client):
        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]

        codename = self._run_remote(
            client,
            "lsb_release -cs",
            "Detecting Ubuntu release"
        ).strip()
        if not codename:
            codename = "jammy"

        self._run_remote_soft(
            client,
            "sudo rm -f /etc/apt/apt.conf.d/00proxy "
            "/etc/apt/apt.conf.d/99proxy "
            "/etc/apt/apt.conf.d/01proxy || true",
            "Removing old proxy config"
        )
        self._run_remote_soft(
            client,
            "sudo rm -f /etc/apt/sources.list.d/ubuntu.sources || true",
            "Removing cloud apt sources"
        )
        self._run_remote(
            client,
            f"echo 'Acquire::http::Proxy \"http://{jump_host}:3142\";' | "
            f"sudo tee /etc/apt/apt.conf.d/00proxy > /dev/null",
            f"Setting APT proxy to {jump_host}"
        )
        self._run_remote(
            client,
            f"printf '%s\\n' "
            f"'deb http://archive.ubuntu.com/ubuntu {codename} main restricted universe multiverse' "
            f"'deb http://archive.ubuntu.com/ubuntu {codename}-updates main restricted universe multiverse' "
            f"'deb http://security.ubuntu.com/ubuntu {codename}-security main restricted universe multiverse' "
            f"| sudo tee /etc/apt/sources.list > /dev/null",
            f"Writing apt sources ({codename}) via proxy"
        )
        self._run_remote(
            client,
            "sudo apt-get clean",
            "Cleaning apt cache"
        )
        self._run_remote(
            client,
            "sudo DEBIAN_FRONTEND=noninteractive apt-get update",
            "Updating apt index"
        )

    def _inject_hosts_mapping(self, client, host_map):
        if not host_map:
            return

        self._run_remote_soft(
            client,
            "sudo cp /etc/hosts /etc/hosts.backup.$(date +%s) || true",
            "Backing up /etc/hosts"
        )

        for name, ip in host_map.items():
            self._run_remote_soft(
                client,
                f"sudo sed -i '/ {name}\\.internal$/d' /etc/hosts || true"
            )
            self._run_remote(
                client,
                f"echo '{ip} {name}.internal' | sudo tee -a /etc/hosts > /dev/null",
                f"Adding host mapping {name}.internal -> {ip}"
            )

    def _replace_ip_in_configs(self, client, old_ip, new_value):
        safe_paths = "/etc /var/www /root"

        self._run_remote_soft(
            client,
            f"sudo grep -rl '{old_ip}' {safe_paths} 2>/dev/null | "
            f"while read f; do "
            f"sudo sed -i 's/{old_ip}/{new_value}/g' \"$f\"; "
            f"done",
            f"Replacing {old_ip} -> {new_value}"
        )

    def restore_mariadb(self, ip, private_key_path):
        logger.info(f"Restoring MariaDB on {ip}...")

        client, jump = self._get_ssh_client(ip, private_key_path)

        try:
            self._fix_apt_sources(client)

            self._run_remote(
                client,
                "sudo DEBIAN_FRONTEND=noninteractive "
                "apt-get install -y mariadb-server",
                "Installing mariadb-server"
            )

            stdin_vdb, stdout_vdb, stderr_vdb = client.exec_command(
                "ls /dev/vdb 2>/dev/null"
            )
            vdb_exists = stdout_vdb.channel.recv_exit_status() == 0

            if vdb_exists:
                logger.info("  Cinder volume detected, configuring...")

                self._run_remote_soft(
                    client,
                    "sudo systemctl stop mariadb",
                    "Stopping MariaDB to setup volume"
                )

                self._run_remote(
                    client,
                    "sudo mkfs.ext4 -F /dev/vdb",
                    "Formatting Cinder volume"
                )

                self._run_remote(
                    client,
                    "sudo mkdir -p /mnt/mariadb-data",
                    "Creating mount point"
                )

                self._run_remote(
                    client,
                    "sudo mount /dev/vdb /mnt/mariadb-data",
                    "Mounting Cinder volume"
                )

                self._run_remote(
                    client,
                    "sudo cp -a /var/lib/mysql/. /mnt/mariadb-data/",
                    "Copying MariaDB data to volume"
                )

                self._run_remote(
                    client,
                    "sudo umount /mnt/mariadb-data",
                    "Unmounting temporary mount"
                )

                self._run_remote(
                    client,
                    "sudo mount /dev/vdb /var/lib/mysql",
                    "Mounting volume to /var/lib/mysql"
                )

                self._run_remote(
                    client,
                    "sudo chown -R mysql:mysql /var/lib/mysql",
                    "Fixing permissions"
                )

                self._run_remote_soft(
                    client,
                    "sudo sed -i '\\|/dev/vdb /var/lib/mysql|d' /etc/fstab || true"
                )

                self._run_remote(
                    client,
                    "echo '/dev/vdb /var/lib/mysql ext4 defaults 0 2' "
                    "| sudo tee -a /etc/fstab > /dev/null",
                    "Adding volume to fstab for auto-mount"
                )

            else:
                logger.warning("  No Cinder volume found, using instance disk")

            self._run_remote_soft(
                client,
                "sudo systemctl start mariadb",
                "Starting MariaDB"
            )

            self._run_remote(
                client,
                "sudo mysql -u root < /tmp/mariadb_dump.sql",
                "Importing database dump"
            )

            self._run_remote(
                client,
                "sudo mysql -u root -e \""
                "CREATE USER IF NOT EXISTS 'appuser'@'%' "
                "IDENTIFIED BY 'password'; "
                "GRANT ALL PRIVILEGES ON *.* TO 'appuser'@'%'; "
                "DROP USER IF EXISTS 'appuser'@'10.0.3.20'; "
                "DROP USER IF EXISTS 'appuser'@'10.0.3.30'; "
                "FLUSH PRIVILEGES;\"",
                "Fixing user grants for new network"
            )

            self._run_remote(
                client,
                "sudo sed -i 's/^bind-address.*/bind-address = 0.0.0.0/' "
                "/etc/mysql/mariadb.conf.d/50-server.cnf",
                "Setting bind-address to 0.0.0.0"
            )

            self._run_remote(
                client,
                "sudo systemctl restart mariadb",
                "Restarting MariaDB"
            )

        finally:
            client.close()
            if jump:
                jump.close()

        logger.info("MariaDB restoration complete")

    def restore_apache(self, ip, private_key_path):
        logger.info(f"Restoring Apache on {ip}...")

        client, jump = self._get_ssh_client(ip, private_key_path)

        try:
            self._fix_apt_sources(client)

            self._run_remote(
                client,
                "sudo DEBIAN_FRONTEND=noninteractive "
                "apt-get install -y apache2 php php-mysql libapache2-mod-php",
                "Installing Apache and PHP"
            )

            self._run_remote(
                client,
                "sudo tar xzf /tmp/apache_backup.tar.gz -C /",
                "Extracting Apache backup"
            )

            self._run_remote_soft(
                client,
                "sudo rm -f /var/www/html/index.html",
                "Removing default Apache page"
            )

            self._run_remote_soft(
                client,
                "sudo a2enmod ssl rewrite headers 2>/dev/null || true",
                "Enabling common modules"
            )

            modules_output = self._run_remote_soft(
                client,
                "cat /tmp/apache_modules.txt",
                "Reading module list"
            )

            for line in modules_output.split("\n"):
                line = line.strip()
                if "_module" in line and "(" in line:
                    module_name = line.split("_module")[0].strip()
                    self._run_remote_soft(
                        client,
                        f"sudo a2enmod {module_name} 2>/dev/null || true"
                    )

            self._run_remote_soft(
                client,
                "sudo a2ensite *.conf 2>/dev/null || true",
                "Enabling virtual hosts"
            )

            self._run_remote(
                client,
                "sudo systemctl restart apache2",
                "Restarting Apache"
            )

        finally:
            client.close()
            if jump:
                jump.close()

        logger.info("Apache restoration complete")

    def restore_backup(self, ip, private_key_path):
        logger.info(f"Restoring Backup service on {ip}...")

        client, jump = self._get_ssh_client(ip, private_key_path)

        try:
            self._fix_apt_sources(client)

            self._run_remote(
                client,
                "sudo DEBIAN_FRONTEND=noninteractive "
                "apt-get install -y mariadb-client",
                "Installing mariadb-client"
            )

            self._run_remote_soft(
                client,
                "sudo cp /tmp/backup_script.sh /root/backup.sh && "
                "sudo chmod +x /root/backup.sh",
                "Installing backup script"
            )

            self._run_remote_soft(
                client,
                "sudo crontab /tmp/backup_crontab.txt",
                "Installing crontab"
            )

        finally:
            client.close()
            if jump:
                jump.close()

        logger.info("Backup service restoration complete")

    def restore_nfs(self, ip, private_key_path, shared_dirs):
        logger.info(f"Restoring NFS on {ip}...")

        client, jump = self._get_ssh_client(ip, private_key_path)

        try:
            self._fix_apt_sources(client)

            self._run_remote(
                client,
                "sudo DEBIAN_FRONTEND=noninteractive "
                "apt-get install -y nfs-kernel-server",
                "Installing NFS server"
            )

            self._run_remote(
                client,
                "sudo cp /tmp/nfs_exports.txt /etc/exports",
                "Restoring /etc/exports"
            )

            for directory in shared_dirs:
                self._run_remote(
                    client,
                    f"sudo mkdir -p {directory}",
                    f"Creating shared directory {directory}"
                )

            self._run_remote_soft(
                client,
                "sudo tar xzf /tmp/nfs_data.tar.gz -C /",
                "Extracting NFS data"
            )

            self._run_remote(
                client,
                "sudo exportfs -ra",
                "Applying NFS exports"
            )

            self._run_remote(
                client,
                "sudo systemctl restart nfs-kernel-server",
                "Restarting NFS server"
            )

        finally:
            client.close()
            if jump:
                jump.close()

        logger.info("NFS restoration complete")

    def restore_ftp(self, ip, private_key_path, server_type):
        logger.info(f"Restoring FTP ({server_type}) on {ip}...")

        client, jump = self._get_ssh_client(ip, private_key_path)

        try:
            self._fix_apt_sources(client)

            self._run_remote(
                client,
                f"sudo DEBIAN_FRONTEND=noninteractive "
                f"apt-get install -y {server_type}",
                f"Installing {server_type}"
            )

            if server_type == "vsftpd":
                config_dest = "/etc/vsftpd.conf"
            else:
                config_dest = "/etc/proftpd/proftpd.conf"
                self._run_remote(
                    client,
                    "sudo mkdir -p /etc/proftpd"
                )

            self._run_remote(
                client,
                f"sudo cp /tmp/ftp_config.conf {config_dest}",
                f"Restoring FTP config to {config_dest}"
            )

            users_output = self._run_remote_soft(
                client,
                "cat /tmp/ftp_users.txt",
                "Reading FTP users"
            )

            for line in users_output.split("\n"):
                parts = line.split(":")
                if len(parts) >= 3:
                    try:
                        uid = int(parts[2])
                        username = parts[0]
                        if uid >= 1000 and username != "nobody":
                            self._run_remote_soft(
                                client,
                                f"sudo useradd -m {username} 2>/dev/null || true"
                            )
                    except ValueError:
                        pass

            self._run_remote_soft(
                client,
                "sudo tar xzf /tmp/ftp_data.tar.gz -C /",
                "Extracting FTP data"
            )

            self._run_remote(
                client,
                f"sudo systemctl restart {server_type}",
                f"Restarting {server_type}"
            )

        finally:
            client.close()
            if jump:
                jump.close()

        logger.info("FTP restoration complete")

    def update_ip_mappings(self, ip, private_key_path, ip_map, service_host_map=None):
        logger.info(f"Updating IP mappings on {ip}...")

        client, jump = self._get_ssh_client(ip, private_key_path)

        try:
            if service_host_map:
                self._inject_hosts_mapping(client, service_host_map)

            for old_ip, new_ip in ip_map.items():
                self._replace_ip_in_configs(client, old_ip, new_ip)

            self._run_remote_soft(
                client,
                "sudo systemctl restart apache2 2>/dev/null || true"
            )

            self._run_remote_soft(
                client,
                "sudo systemctl restart mariadb 2>/dev/null || true"
            )

            self._run_remote_soft(
                client,
                "sudo systemctl restart nfs-kernel-server 2>/dev/null || true"
            )

            self._run_remote_soft(
                client,
                "sudo systemctl restart vsftpd 2>/dev/null || true"
            )

            self._run_remote_soft(
                client,
                "sudo systemctl restart proftpd 2>/dev/null || true"
            )

        finally:
            client.close()
            if jump:
                jump.close()

        logger.info("IP mappings updated")

    def restore_all(self, inventory, backup_paths, private_key_path, ports):
        ip_map = {}
        service_host_map = {}

        for container in inventory:
            name = container["name"]

            if name in ports:
                old_ip = container["ip"]
                new_ip = ports[name].fixed_ips[0]["ip_address"]

                service_host_map[name] = new_ip

                if old_ip != new_ip:
                    ip_map[old_ip] = new_ip

        subprocess.run(
            "sudo sysctl -w net.ipv4.ip_forward=1",
            shell=True,
            capture_output=True
        )

        provider_interface = self.config.get(
            "openstack",
            {}
        ).get(
            "provider_interface",
            "ens35"
        )

        provider_cidr = self.config.get(
            "openstack",
            {}
        ).get(
            "provider_cidr",
            "10.0.0.0/24"
        )

        subprocess.run(
            f"sudo iptables -t nat -C POSTROUTING "
            f"-s {provider_cidr} -o {provider_interface} -j MASQUERADE "
            f"2>/dev/null || "
            f"sudo iptables -t nat -A POSTROUTING "
            f"-s {provider_cidr} -o {provider_interface} -j MASQUERADE",
            shell=True,
            capture_output=True
        )

        logger.info(
            f"NAT routing enabled for instances "
            f"via {provider_interface}"
        )
        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_user = self.config["jump"]["username"]
        jump_password = self.config["jump"]["password"]

        proxy_client = paramiko.SSHClient()
        proxy_client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )
        proxy_client.connect(
            hostname=jump_host,
            username=jump_user,
            password=jump_password,
            timeout=10
        )
        stdin, stdout, stderr = proxy_client.exec_command(
            "sudo DEBIAN_FRONTEND=noninteractive "
            "apt-get install -y apt-cacher-ng && "
            "sudo systemctl start apt-cacher-ng"
        )
        stdout.channel.recv_exit_status()
        proxy_client.close()
        logger.info("APT proxy (apt-cacher-ng) ready on vm-cible")

        for container in inventory:
            name = container["name"]
            service = container["service"]

            if name not in ports:
                continue

            port = ports[name]
            ip = port.fixed_ips[0]["ip_address"]

            if service == "mariadb":
                self.restore_mariadb(ip, private_key_path)

            elif service == "apache":
                self.restore_apache(ip, private_key_path)

            elif service == "backup":
                self.restore_backup(ip, private_key_path)

            elif service == "nfs":
                shared_dirs = []
                if name in backup_paths:
                    shared_dirs = backup_paths[name].get("shared_dirs", [])

                self.restore_nfs(ip, private_key_path, shared_dirs)

            elif service == "ftp":
                server_type = "vsftpd"
                if name in backup_paths:
                    server_type = backup_paths[name].get(
                        "server_type",
                        "vsftpd"
                    )

                self.restore_ftp(ip, private_key_path, server_type)

        if ip_map or service_host_map:
            logger.info("Updating IP configurations...")

            for container in inventory:
                name = container["name"]

                if name not in ports:
                    continue

                port = ports[name]
                ip = port.fixed_ips[0]["ip_address"]

                self.update_ip_mappings(
                    ip,
                    private_key_path,
                    ip_map,
                    service_host_map
                )

        logger.info("All restorations complete")
