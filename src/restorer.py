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
        jump_client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )
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
        target_client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy()
        )
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
            logger.error(
                f"  Command failed (exit {exit_code}): {command}"
            )
            if errors:
                logger.error(f"  STDERR: {errors}")
            raise Exception(
                f"Remote command failed: {command}"
            )

        if output:
            logger.debug(f"  OUTPUT: {output[:200]}")
        return output

    def _fix_apt_sources(self, client):
        source_ip = self.config["source"]["host"]
        from src.scanner import run_command
        mirror = run_command(
            "grep -m1 '^deb ' /etc/apt/sources.list 2>/dev/null | awk '{print $2}'"
        )
        if not mirror:
            mirror = run_command(
                "grep -m1 'URIs:' /etc/apt/sources.list.d/*.sources 2>/dev/null | awk '{print $2}'"
            )
        if not mirror:
            mirror = "http://archive.ubuntu.com/ubuntu"
        mirror = mirror.rstrip("/")

        codename = run_command("lsb_release -cs")
        if not codename:
            codename = "noble"

        logger.info(f"Using mirror: {mirror} ({codename})")

        self._run_remote(
            client,
            "sudo rm -f /etc/apt/sources.list.d/ubuntu.sources",
            "Removing cloud apt sources"
        )
        self._run_remote(
            client,
            "sudo mkdir -p /etc/cloud/cloud.cfg.d",
        )
        self._run_remote(
            client,
            "echo 'apt_preserve_sources_list: true' | sudo tee /etc/cloud/cloud.cfg.d/99-disable-apt.cfg > /dev/null",
        )
        self._run_remote(
            client,
            f"echo 'Acquire::http::Proxy \"http://{source_ip}:3142\";' | sudo tee /etc/apt/apt.conf.d/00proxy > /dev/null",
            "Configuring APT proxy via source host"
        )
        self._run_remote(
            client,
            f"echo 'deb {mirror} {codename} main restricted universe multiverse' | sudo tee /etc/apt/sources.list > /dev/null",
        )
        self._run_remote(
            client,
            f"echo 'deb {mirror} {codename}-updates main restricted universe multiverse' | sudo tee -a /etc/apt/sources.list > /dev/null",
        )
        self._run_remote(
            client,
            f"echo 'deb {mirror} {codename}-security main restricted universe multiverse' | sudo tee -a /etc/apt/sources.list > /dev/null",
            f"Writing apt sources ({mirror})"
        )

    def restore_mariadb(self, ip, private_key_path):
        logger.info(f"Restoring MariaDB on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self._fix_apt_sources(client)

        self._run_remote(
            client,
            "sudo apt update && sudo apt install -y mariadb-server",
            "Installing mariadb-server"
        )

        self._run_remote(
            client,
            "sudo systemctl stop mariadb",
            "Stopping MariaDB to setup volume"
        )

        self._run_remote(
            client,
            "sudo mkfs.ext4 /dev/vdb",
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
            "sudo cp -a /var/lib/mysql/* /mnt/mariadb-data/",
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

        self._run_remote(
            client,
            "echo '/dev/vdb /var/lib/mysql ext4 defaults 0 2' "
            "| sudo tee -a /etc/fstab",
            "Adding volume to fstab for auto-mount"
        )

        self._run_remote(
            client,
            "sudo systemctl start mariadb",
            "Starting MariaDB on Cinder volume"
        )

        self._run_remote(
            client,
            "sudo mysql -u root < /tmp/mariadb_dump.sql",
            "Importing database dump"
        )

        self._run_remote(
            client,
            "sudo sed -i 's/bind-address.*/bind-address = 0.0.0.0/' "
            "/etc/mysql/mariadb.conf.d/50-server.cnf",
            "Setting bind-address to 0.0.0.0"
        )

        self._run_remote(
            client,
            "sudo systemctl restart mariadb",
            "Restarting MariaDB"
        )

        client.close()
        if jump:
            jump.close()
        logger.info("MariaDB restoration complete (Cinder volume)")

    def restore_apache(self, ip, private_key_path):
        logger.info(f"Restoring Apache on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self._fix_apt_sources(client)

        self._run_remote(
            client,
            "sudo apt update && sudo apt install -y "
            "apache2 php php-mysql libapache2-mod-php",
            "Installing Apache and PHP"
        )

        self._run_remote(
            client,
            "sudo tar xzf /tmp/apache_backup.tar.gz -C /",
            "Extracting Apache backup"
        )

        modules_output = self._run_remote(
            client,
            "cat /tmp/apache_modules.txt",
            "Reading module list"
        )
        for line in modules_output.split("\n"):
            line = line.strip()
            if "_module" in line and "(" in line:
                module_name = line.split("_module")[0].strip()
                try:
                    self._run_remote(
                        client,
                        f"sudo a2enmod {module_name} 2>/dev/null"
                    )
                except Exception:
                    pass

        self._run_remote(
            client,
            "sudo a2ensite *.conf 2>/dev/null || true",
            "Enabling virtual hosts"
        )

        self._run_remote(
            client,
            "sudo systemctl restart apache2",
            "Restarting Apache"
        )

        client.close()
        if jump:
            jump.close()
        logger.info("Apache restoration complete")

    def restore_backup(self, ip, private_key_path):
        logger.info(f"Restoring Backup service on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self._fix_apt_sources(client)

        self._run_remote(
            client,
            "sudo apt update && sudo apt install -y mariadb-client",
            "Installing mariadb-client"
        )

        try:
            self._run_remote(
                client,
                "sudo cp /tmp/backup_script.sh /root/backup.sh && "
                "sudo chmod +x /root/backup.sh",
                "Installing backup script"
            )
        except Exception:
            logger.warning("No backup script found, skipping")

        try:
            self._run_remote(
                client,
                "sudo crontab /tmp/backup_crontab.txt",
                "Installing crontab"
            )
        except Exception:
            logger.warning("No crontab found, skipping")

        client.close()
        if jump:
            jump.close()
        logger.info("Backup service restoration complete")

    def restore_nfs(self, ip, private_key_path, shared_dirs):
        logger.info(f"Restoring NFS on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self._fix_apt_sources(client)

        self._run_remote(
            client,
            "sudo apt update && sudo apt install -y nfs-kernel-server",
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

        try:
            self._run_remote(
                client,
                "sudo tar xzf /tmp/nfs_data.tar.gz -C /",
                "Extracting NFS data"
            )
        except Exception:
            logger.warning("No NFS data archive, skipping")

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

        client.close()
        if jump:
            jump.close()
        logger.info("NFS restoration complete")

    def restore_ftp(self, ip, private_key_path, server_type):
        logger.info(f"Restoring FTP ({server_type}) on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self._fix_apt_sources(client)

        self._run_remote(
            client,
            f"sudo apt update && sudo apt install -y {server_type}",
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

        users_output = self._run_remote(
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
                        try:
                            self._run_remote(
                                client,
                                f"sudo useradd -m {username} 2>/dev/null"
                            )
                        except Exception:
                            pass
                except ValueError:
                    pass

        try:
            self._run_remote(
                client,
                "sudo tar xzf /tmp/ftp_data.tar.gz -C /",
                "Extracting FTP data"
            )
        except Exception:
            logger.warning("No FTP data archive, skipping")

        self._run_remote(
            client,
            f"sudo systemctl restart {server_type}",
            f"Restarting {server_type}"
        )

        client.close()
        if jump:
            jump.close()
        logger.info("FTP restoration complete")

    def update_ip_mappings(self, ip, private_key_path, ip_map):
        logger.info(f"Updating IP mappings on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        for old_ip, new_ip in ip_map.items():
            self._run_remote(
                client,
                f"sudo grep -rl '{old_ip}' /etc/ /var/www/ "
                f"/root/ 2>/dev/null | while read f; do "
                f"sudo sed -i 's/{old_ip}/{new_ip}/g' \"$f\"; done",
                f"Replacing {old_ip} -> {new_ip}"
            )

        client.close()
        if jump:
            jump.close()
        logger.info("IP mappings updated")

    def restore_all(self, inventory, backup_paths,
                    private_key_path, ports):
        ip_map = {}
        for container in inventory:
            name = container["name"]
            if name in ports:
                old_ip = container["ip"]
                new_ip = ports[name].fixed_ips[0]["ip_address"]
                if old_ip != new_ip:
                    ip_map[old_ip] = new_ip

        subprocess.run(
            "sudo sysctl -w net.ipv4.ip_forward=1",
            shell=True, capture_output=True
        )
        subprocess.run(
            "sudo iptables -t nat -C POSTROUTING -s 10.0.0.0/24 -o ens33 -j MASQUERADE 2>/dev/null || "
            "sudo iptables -t nat -A POSTROUTING -s 10.0.0.0/24 -o ens33 -j MASQUERADE",
            shell=True, capture_output=True
        )
        logger.info("NAT routing enabled for instances")

        subprocess.run(
            "sudo systemctl start apt-cacher-ng 2>/dev/null || true",
            shell=True, capture_output=True
        )
        logger.info("APT proxy enabled on source host")

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
                    shared_dirs = backup_paths[name].get(
                        "shared_dirs", []
                    )
                self.restore_nfs(ip, private_key_path, shared_dirs)
            elif service == "ftp":
                server_type = "vsftpd"
                if name in backup_paths:
                    server_type = backup_paths[name].get(
                        "server_type", "vsftpd"
                    )
                self.restore_ftp(ip, private_key_path, server_type)

        if ip_map:
            logger.info("Updating IP configurations...")
            for container in inventory:
                name = container["name"]
                if name not in ports:
                    continue
                port = ports[name]
                ip = port.fixed_ips[0]["ip_address"]
                self.update_ip_mappings(ip, private_key_path, ip_map)

        logger.info("All restorations complete")
