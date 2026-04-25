import logging
import paramiko


logger = logging.getLogger("migration")


class Validator:

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
            f"echo '{escaped_password}' | sudo -S -p '' chmod 600 {remote_key_path}"
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

    def _run_remote(self, client, command):
        stdin, stdout, stderr = client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode().strip()
        return exit_code, output

    def _run_remote_tenant(self, ip, private_key_path, command):
        jump_client = self._connect_jump_host()

        try:
            remote_key_path = self._copy_key_to_jump_host(
                jump_client,
                private_key_path
            )
            namespace = self._get_router_namespace(jump_client)

            jump_password = self.config["jump"]["password"]
            escaped_password = self._escape_single_quotes(jump_password)
            escaped_command = self._escape_single_quotes(command)

            tenant_command = (
                f"echo '{escaped_password}' | sudo -S -p '' "
                f"ip netns exec {namespace} "
                f"ssh -o StrictHostKeyChecking=no "
                f"-o UserKnownHostsFile=/dev/null "
                f"-o ConnectTimeout=10 "
                f"-i {remote_key_path} "
                f"ubuntu@{ip} '{escaped_command}'"
            )

            exit_code, output, errors = self._run_jump_command(
                jump_client,
                tenant_command
            )

            if errors:
                logger.debug(f"Tenant validation stderr for {ip}: {errors}")

            return exit_code, output

        finally:
            jump_client.close()

    def _run_remote_on_instance(self, ip, private_key_path, command):
        if self._is_tenant_ip(ip):
            return self._run_remote_tenant(ip, private_key_path, command)

        client, jump = self._get_ssh_client(ip, private_key_path)
        try:
            return self._run_remote(client, command)
        finally:
            client.close()
            if jump:
                jump.close()

    def validate_mariadb(self, ip, private_key_path, source_count):
        logger.info(f"Validating MariaDB on {ip}...")

        exit_code, output = self._run_remote_on_instance(
            ip,
            private_key_path,
            "mysql -u appuser -ppassword -h 127.0.0.1 -e "
            "\"SELECT COUNT(*) FROM app_db.users\""
        )

        if exit_code != 0:
            logger.error("MariaDB validation FAILED: cannot connect")
            return False

        lines = output.split("\n")
        if len(lines) >= 2:
            target_count = lines[1].strip()
            if target_count == str(source_count):
                logger.info(
                    f"MariaDB validation PASSED "
                    f"({target_count} rows match)"
                )
                return True
            else:
                logger.error(
                    f"MariaDB validation FAILED: "
                    f"source={source_count}, target={target_count}"
                )
                return False

        logger.error("MariaDB validation FAILED: unexpected output")
        return False

    def validate_apache(self, ip, private_key_path):
        logger.info(f"Validating Apache on {ip}...")

        exit_code, output = self._run_remote_on_instance(
            ip,
            private_key_path,
            "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1"
        )

        if exit_code == 0 and output == "200":
            logger.info("Apache validation PASSED (HTTP 200)")
            return True

        logger.error(f"Apache validation FAILED: HTTP {output}")
        return False

    def validate_backup(self, ip, private_key_path, mariadb_ip):
        logger.info(f"Validating Backup service on {ip}...")

        exit_code_cron, crontab = self._run_remote_on_instance(
            ip,
            private_key_path,
            "sudo crontab -l 2>/dev/null"
        )

        exit_code_dump, dump_path = self._run_remote_on_instance(
            ip,
            private_key_path,
            "which mysqldump"
        )

        exit_code_conn, conn_test = self._run_remote_on_instance(
            ip,
            private_key_path,
            f"mysqldump -u appuser -ppassword -h {mariadb_ip} "
            f"--no-data app_db 2>/dev/null | head -1"
        )

        cron_ok = exit_code_cron == 0 and len(crontab) > 0
        dump_ok = exit_code_dump == 0
        conn_ok = exit_code_conn == 0

        if cron_ok and dump_ok and conn_ok:
            logger.info("Backup validation PASSED")
            return True

        if not cron_ok:
            logger.error("Backup validation: crontab missing")
        if not dump_ok:
            logger.error("Backup validation: mysqldump not installed")
        if not conn_ok:
            logger.error(
                f"Backup validation: cannot reach MariaDB at {mariadb_ip}"
            )
        return False

    def validate_nfs(self, ip, private_key_path):
        logger.info(f"Validating NFS on {ip}...")

        exit_code_service, output = self._run_remote_on_instance(
            ip,
            private_key_path,
            "sudo systemctl is-active nfs-kernel-server"
        )

        exit_code_exports, exports = self._run_remote_on_instance(
            ip,
            private_key_path,
            "sudo exportfs -v"
        )

        service_ok = exit_code_service == 0 and output == "active"
        exports_ok = exit_code_exports == 0 and len(exports) > 0

        if service_ok and exports_ok:
            logger.info("NFS validation PASSED")
            return True

        if not service_ok:
            logger.error("NFS validation: service not running")
        if not exports_ok:
            logger.error("NFS validation: no exports found")
        return False

    def validate_ftp(self, ip, private_key_path, server_type):
        logger.info(f"Validating FTP ({server_type}) on {ip}...")

        exit_code_service, output = self._run_remote_on_instance(
            ip,
            private_key_path,
            f"sudo systemctl is-active {server_type}"
        )

        exit_code_port, port_check = self._run_remote_on_instance(
            ip,
            private_key_path,
            "ss -tlnp | grep ':21 '"
        )

        service_ok = exit_code_service == 0 and output == "active"
        port_ok = exit_code_port == 0 and len(port_check) > 0

        if service_ok and port_ok:
            logger.info("FTP validation PASSED")
            return True

        if not service_ok:
            logger.error(f"FTP validation: {server_type} not running")
        if not port_ok:
            logger.error("FTP validation: port 21 not listening")
        return False

    def validate_all(self, inventory, backup_paths,
                     private_key_path, ports):
        results = {}
        mariadb_ip = None

        for container in inventory:
            if container["service"] == "mariadb":
                if container["name"] in ports:
                    mariadb_ip = ports[container["name"]].fixed_ips[0]["ip_address"]
                break

        for container in inventory:
            name = container["name"]
            service = container["service"]

            if name not in ports:
                continue

            port = ports[name]
            ip = port.fixed_ips[0]["ip_address"]

            try:
                if service == "mariadb":
                    source_count = self._get_source_count(name)
                    results[name] = self.validate_mariadb(
                        ip,
                        private_key_path,
                        source_count
                    )
                elif service == "apache":
                    results[name] = self.validate_apache(
                        ip,
                        private_key_path
                    )
                elif service == "backup":
                    results[name] = self.validate_backup(
                        ip,
                        private_key_path,
                        mariadb_ip
                    )
                elif service == "nfs":
                    results[name] = self.validate_nfs(
                        ip,
                        private_key_path
                    )
                elif service == "ftp":
                    server_type = "vsftpd"
                    if name in backup_paths:
                        server_type = backup_paths[name].get(
                            "server_type",
                            "vsftpd"
                        )
                    results[name] = self.validate_ftp(
                        ip,
                        private_key_path,
                        server_type
                    )
            except Exception as e:
                logger.error(f"Validation error for {name}: {e}")
                results[name] = False

        logger.info("=" * 40)
        logger.info("MIGRATION RESULTS")
        logger.info("=" * 40)
        for name, passed in results.items():
            status = "PASSED" if passed else "FAILED"
            logger.info(f"  {name:12s} : {status}")
        logger.info("=" * 40)

        return results

    def _get_source_count(self, container_name):
        from src.scanner import run_command
        output = run_command(
            f"sudo lxc-attach -n {container_name} -- "
            f"mysql -u root -N -e "
            f"\"SELECT COUNT(*) FROM app_db.users\""
        )
        try:
            return int(output.strip())
        except ValueError:
            logger.warning("Could not get source count, using 0")
            return 0
