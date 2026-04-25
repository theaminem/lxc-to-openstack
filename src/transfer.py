import os
import shlex
import logging
import posixpath
import paramiko


logger = logging.getLogger("migration")


class Transfer:

    def __init__(self, config):
        self.config = config

    def _get_jump_info(self):
        jump_host = self.config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        jump_user = self.config["jump"]["username"]
        jump_password = self.config["jump"]["password"]
        return jump_host, jump_user, jump_password

    def _connect_jump_host(self):
        jump_host, jump_user, jump_password = self._get_jump_info()

        jump_client = paramiko.SSHClient()
        jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jump_client.connect(
            hostname=jump_host,
            username=jump_user,
            password=jump_password,
            timeout=10
        )
        return jump_client

    def _is_tenant_ip(self, ip):
        source_subnet = self.config.get("source", {}).get("bridge_subnet", "")
        if source_subnet.startswith("10.0.3."):
            return ip.startswith("10.0.3.")
        return ip.startswith("10.0.3.")

    def _escape_single_quotes(self, value):
        return value.replace("'", "'\"'\"'")

    def _copy_key_to_jump_host(self, jump_client, private_key_path):
        remote_key_path = "/tmp/migration-key"

        sftp = jump_client.open_sftp()
        sftp.put(private_key_path, remote_key_path)
        sftp.close()

        jump_password = self.config["jump"]["password"]
        escaped_password = self._escape_single_quotes(jump_password)

        stdin, stdout, stderr = jump_client.exec_command(
            f"echo '{escaped_password}' | sudo -S chmod 600 {remote_key_path}"
        )
        exit_code = stdout.channel.recv_exit_status()
        errors = stderr.read().decode().strip()

        if exit_code != 0:
            raise Exception(f"Failed to chmod key on vm-cible: {errors}")

        return remote_key_path

    def _get_router_namespace(self, jump_client):
        stdin, stdout, stderr = jump_client.exec_command(
            "ip netns | awk '/qrouter/ {print $1; exit}'"
        )
        namespace = stdout.read().decode().strip()

        if not namespace:
            raise Exception("No qrouter namespace found on vm-cible")

        return namespace

    def _run_jump_command(self, jump_client, command):
        stdin, stdout, stderr = jump_client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode().strip()
        errors = stderr.read().decode().strip()
        return exit_code, output, errors

    def _get_ssh_client(self, ip, private_key_path):
        jump_host, jump_user, jump_password = self._get_jump_info()

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

    def upload_file_via_tenant_namespace(self, ip, private_key_path, local_path, remote_path):
        if not os.path.exists(local_path):
            logger.warning(f"File not found, skipping: {local_path}")
            return False

        jump_client = self._connect_jump_host()

        try:
            remote_key_path = self._copy_key_to_jump_host(
                jump_client,
                private_key_path
            )
            namespace = self._get_router_namespace(jump_client)

            file_size = os.path.getsize(local_path)
            remote_tmp_path = f"/tmp/{os.path.basename(local_path)}"

            logger.info(
                f"Uploading {local_path} -> {ip}:{remote_path} via {namespace} "
                f"({self._file_size(file_size)})"
            )

            sftp = jump_client.open_sftp()
            sftp.put(local_path, remote_tmp_path)
            sftp.close()

            quoted_tmp = shlex.quote(remote_tmp_path)
            quoted_remote = shlex.quote(remote_path)
            jump_password = self.config["jump"]["password"]
            escaped_password = self._escape_single_quotes(jump_password)

            remote_dir = posixpath.dirname(remote_path)
            mkdir_cmd = (
                f"echo '{escaped_password}' | sudo -S ip netns exec {namespace} "
                f"ssh -o StrictHostKeyChecking=no "
                f"-o UserKnownHostsFile=/dev/null "
                f"-i {remote_key_path} ubuntu@{ip} "
                f"'mkdir -p {shlex.quote(remote_dir)}'"
            )
            exit_code, output, errors = self._run_jump_command(
                jump_client,
                mkdir_cmd
            )
            if exit_code != 0:
                raise Exception(f"Failed to create remote directory: {errors}")

            scp_cmd = (
                f"echo '{escaped_password}' | sudo -S ip netns exec {namespace} "
                f"scp -o StrictHostKeyChecking=no "
                f"-o UserKnownHostsFile=/dev/null "
                f"-i {remote_key_path} {quoted_tmp} ubuntu@{ip}:{quoted_remote}"
            )
            exit_code, output, errors = self._run_jump_command(
                jump_client,
                scp_cmd
            )

            cleanup_cmd = f"rm -f {quoted_tmp}"
            self._run_jump_command(jump_client, cleanup_cmd)

            if exit_code != 0:
                raise Exception(f"Tenant upload failed: {errors}")

            logger.info(f"Upload complete: {remote_path}")
            return True

        finally:
            jump_client.close()

    def _upload_file_to_instance(self, ip, private_key_path, local_path, remote_path):
        if self._is_tenant_ip(ip):
            return self.upload_file_via_tenant_namespace(
                ip,
                private_key_path,
                local_path,
                remote_path
            )

        client, jump = self._get_ssh_client(ip, private_key_path)
        try:
            return self.upload_file(client, local_path, remote_path)
        finally:
            client.close()
            if jump:
                jump.close()

    def transfer_mariadb(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring MariaDB data to {ip}...")

        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["dump"],
            "/tmp/mariadb_dump.sql"
        )
        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["users"],
            "/tmp/mariadb_users.sql"
        )

        logger.info("MariaDB transfer complete")

    def transfer_apache(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring Apache data to {ip}...")

        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["archive"],
            "/tmp/apache_backup.tar.gz"
        )
        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["modules"],
            "/tmp/apache_modules.txt"
        )

        logger.info("Apache transfer complete")

    def transfer_backup(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring Backup data to {ip}...")

        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["crontab"],
            "/tmp/backup_crontab.txt"
        )
        if backup_paths.get("script"):
            self._upload_file_to_instance(
                ip,
                private_key_path,
                backup_paths["script"],
                "/tmp/backup_script.sh"
            )

        logger.info("Backup transfer complete")

    def transfer_nfs(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring NFS data to {ip}...")

        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["exports"],
            "/tmp/nfs_exports.txt"
        )
        if backup_paths.get("data_archive"):
            self._upload_file_to_instance(
                ip,
                private_key_path,
                backup_paths["data_archive"],
                "/tmp/nfs_data.tar.gz"
            )

        logger.info("NFS transfer complete")

    def transfer_ftp(self, ip, private_key_path, backup_paths):
        logger.info(f"Transferring FTP data to {ip}...")

        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["config"],
            "/tmp/ftp_config.conf"
        )
        self._upload_file_to_instance(
            ip,
            private_key_path,
            backup_paths["users"],
            "/tmp/ftp_users.txt"
        )
        if backup_paths.get("data_archive"):
            self._upload_file_to_instance(
                ip,
                private_key_path,
                backup_paths["data_archive"],
                "/tmp/ftp_data.tar.gz"
            )

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
