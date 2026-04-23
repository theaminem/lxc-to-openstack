import logging
import paramiko


logger = logging.getLogger("migration")


class Validator:

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
    
    def _run_remote(self, client, command):
        stdin, stdout, stderr = client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode().strip()
        return exit_code, output

    def validate_mariadb(self, ip, private_key_path, source_count):
        logger.info(f"Validating MariaDB on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        exit_code, output = self._run_remote(
            client,
            "sudo mysql -u appuser -ppass123 -e "
            "\"SELECT COUNT(*) FROM app_db.users\""
        )

        client.close()
        if jump:
            jump.close()

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
        client, jump = self._get_ssh_client(ip, private_key_path)

        exit_code, output = self._run_remote(
            client,
            f"curl -s -o /dev/null -w '%{{http_code}}' "
            f"http://127.0.0.1"
        )

        client.close()
        if jump:
            jump.close()

        if exit_code == 0 and output == "200":
            logger.info("Apache validation PASSED (HTTP 200)")
            return True

        logger.error(
            f"Apache validation FAILED: HTTP {output}"
        )
        return False

    def validate_backup(self, ip, private_key_path, mariadb_ip):
        logger.info(f"Validating Backup service on {ip}...")
        client, jump = self._get_ssh_client(ip, private_key_path)

        exit_code_cron, crontab = self._run_remote(
            client,
            "sudo crontab -l 2>/dev/null"
        )
        
        exit_code_dump, dump_path = self._run_remote(
            client,
            "which mysqldump"
        )

        exit_code_conn, conn_test = self._run_remote(
            client,
            f"mysqldump -u appuser -ppass123 -h {mariadb_ip} "
            f"--no-data app_db 2>/dev/null | head -1"
        )

        client.close()
        if jump:
            jump.close()

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
        client, jump = self._get_ssh_client(ip, private_key_path)

        exit_code_service, output = self._run_remote(
            client,
            "sudo systemctl is-active nfs-kernel-server"
        )

        exit_code_exports, exports = self._run_remote(
            client,
            "sudo exportfs -v"
        )

        client.close()
        if jump:
            jump.close()

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
        client, jump = self._get_ssh_client(ip, private_key_path)

        exit_code_service, output = self._run_remote(
            client,
            f"sudo systemctl is-active {server_type}"
        )

        exit_code_port, port_check = self._run_remote(
            client,
            "ss -tlnp | grep ':21 '"
        )

        client.close()
        if jump:
            jump.close()

        service_ok = exit_code_service == 0 and output == "active"
        port_ok = exit_code_port == 0 and len(port_check) > 0

        if service_ok and port_ok:
            logger.info("FTP validation PASSED")
            return True

        if not service_ok:
            logger.error(
                f"FTP validation: {server_type} not running"
            )
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
                        ip, private_key_path, source_count
                    )
                elif service == "apache":
                    results[name] = self.validate_apache(
                        ip, private_key_path
                    )
                elif service == "backup":
                    results[name] = self.validate_backup(
                        ip, private_key_path, mariadb_ip
                    )
                elif service == "nfs":
                    results[name] = self.validate_nfs(
                        ip, private_key_path
                    )
                elif service == "ftp":
                    server_type = "vsftpd"
                    if name in backup_paths:
                        server_type = backup_paths[name].get(
                            "server_type", "vsftpd"
                        )
                    results[name] = self.validate_ftp(
                        ip, private_key_path, server_type
                    )
            except Exception as e:
                logger.error(
                    f"Validation error for {name}: {e}"
                )
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
            logger.warning(
                "Could not get source count, using 0"
            )
            return 0
