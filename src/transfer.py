import os
import logging
import paramiko


logger = logging.getLogger("migration")


class Transfer:

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

    def upload_file(self, ssh_client, local_path, remote_path):
        if not os.path.exists(local_path):
            logger.warning(f"File not found, skipping: {local_path}")
            return False

        sftp = ssh_client.open_sftp()
        file_size = os.path.getsize(local_path)
        logger.info(
            f"Uploading {local_path} -> {remote_path} "
            f"({self._file_size(file_size)})"
        )
        sftp.put(local_path, remote_path)
        sftp.close()
        logger.info(f"Upload complete: {remote_path}")
        return True

    def transfer_mariadb(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring MariaDB data to {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self.upload_file(
            client,
            backup_paths["dump"],
            "/tmp/mariadb_dump.sql"
        )
        self.upload_file(
            client,
            backup_paths["users"],
            "/tmp/mariadb_users.sql"
        )

        client.close()
        if jump:
            jump.close()
        logger.info("MariaDB transfer complete")

    def transfer_apache(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring Apache data to {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self.upload_file(
            client,
            backup_paths["archive"],
            "/tmp/apache_backup.tar.gz"
        )
        self.upload_file(
            client,
            backup_paths["modules"],
            "/tmp/apache_modules.txt"
        )

        client.close()
        if jump:
            jump.close()
        logger.info("Apache transfer complete")

    def transfer_backup(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring Backup data to {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self.upload_file(
            client,
            backup_paths["crontab"],
            "/tmp/backup_crontab.txt"
        )
        if backup_paths.get("script"):
            self.upload_file(
                client,
                backup_paths["script"],
                "/tmp/backup_script.sh"
            )

        client.close()
        if jump:
            jump.close()
        logger.info("Backup transfer complete")

    def transfer_nfs(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring NFS data to {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self.upload_file(
            client,
            backup_paths["exports"],
            "/tmp/nfs_exports.txt"
        )
        if backup_paths.get("data_archive"):
            self.upload_file(
                client,
                backup_paths["data_archive"],
                "/tmp/nfs_data.tar.gz"
            )

        client.close()
        if jump:
            jump.close()
        logger.info("NFS transfer complete")

    def transfer_ftp(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring FTP data to {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        self.upload_file(
            client,
            backup_paths["config"],
            "/tmp/ftp_config.conf"
        )
        self.upload_file(
            client,
            backup_paths["users"],
            "/tmp/ftp_users.txt"
        )
        if backup_paths.get("data_archive"):
            self.upload_file(
                client,
                backup_paths["data_archive"],
                "/tmp/ftp_data.tar.gz"
            )

        client.close()
        if jump:
            jump.close()
        logger.info("FTP transfer complete")

    def _file_size(self, size):
        if size < 1024:
            return f"{size}B"
        elif size < 1024 * 1024:
            return f"{size // 1024}KB"
        else:
            return f"{size // (1024 * 1024)}MB"

    def transfer_all(self, inventory, backup_paths,
                     private_key_path, ports):
        for container in inventory:
            name = container["name"]
            service = container["service"]

            if name not in backup_paths:
                logger.warning(f"No backup for {name}, skipping")
                continue
            if name not in ports:
                continue

            port = ports[name]
            ip = port.fixed_ips[0]["ip_address"]
            paths = backup_paths[name]

            if service == "mariadb":
                self.transfer_mariadb(ip, private_key_path, paths)
            elif service == "apache":
                self.transfer_apache(ip, private_key_path, paths)
            elif service == "backup":
                self.transfer_backup(ip, private_key_path, paths)
            elif service == "nfs":
                self.transfer_nfs(ip, private_key_path, paths)
            elif service == "ftp":
                self.transfer_ftp(ip, private_key_path, paths)

        logger.info("All transfers complete")
