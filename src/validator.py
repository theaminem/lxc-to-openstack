"""
Validator — vérification post-migration des services sur les instances OpenStack.

Améliorations v2 :
- Utilise JumpHostClient (plus de duplication SSH)
- Validation MariaDB générique : vérifie toutes les BDD migrées, pas 'app_db.users'
- Credentials passés en paramètre (pas de hardcode 'appuser/password')
- _is_tenant_ip() supprimé (géré par JumpHostClient)
"""

import logging

from src.jump_client import JumpHostClient

logger = logging.getLogger("migration")


class Validator:

    def __init__(self, config: dict):
        self.config = config

    # -----------------------------------------------------------------------
    # MariaDB — validation générique
    # -----------------------------------------------------------------------

    def _get_source_databases(self, container_name: str,
                               app_databases: list) -> dict:
        """
        For each app database on the source, count rows in every table.
        Returns { "db_name": { "table_name": row_count } }
        """
        from src.scanner import _lxc_attach
        counts = {}
        for db in app_databases:
            counts[db] = {}
            tables_out = _lxc_attach(
                container_name,
                "mysql", "-u", "root", "-N", "-e",
                f"SHOW TABLES IN `{db}`"
            )
            for table in tables_out.split("\n"):
                table = table.strip()
                if not table:
                    continue
                count_out = _lxc_attach(
                    container_name,
                    "mysql", "-u", "root", "-N", "-e",
                    f"SELECT COUNT(*) FROM `{db}`.`{table}`"
                )
                try:
                    counts[db][table] = int(count_out.strip())
                except ValueError:
                    counts[db][table] = 0
        return counts

    def validate_mariadb(self, ip: str, private_key_path: str,
                          container: dict, db_user: str,
                          db_password: str,
                          backup_paths: dict) -> bool:
        logger.info(f"Validating MariaDB on {ip}...")
        app_databases = container.get("app_databases", [])
        container_name = container["name"]

        if not app_databases:
            logger.warning(
                "  No app databases in inventory — skipping MariaDB validation"
            )
            return True

        # Use frozen row counts captured at backup time. If absent
        # (e.g. backup made by an older version), fall back to the live
        # source counts (which may drift if the source kept writing).
        source_counts = backup_paths.get("row_counts")
        if source_counts:
            logger.info(
                "  Using row counts captured at backup time (frozen snapshot)"
            )
        else:
            logger.warning(
                "  No frozen counts in backup_paths — querying live source"
            )
            source_counts = self._get_source_databases(
                container_name, app_databases
            )

        passed = True
        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)

            # Check each database exists on target
            exit_code, dbs_out = self._run(
                jc, client,
                f"mysql -u {db_user} -p'{db_password}' "
                f"-h {ip} -N -e 'SHOW DATABASES'"
            )
            if exit_code != 0:
                logger.error("  MariaDB validation FAILED: cannot connect")
                return False

            target_dbs = set(dbs_out.split("\n"))

            for db, tables in source_counts.items():
                if db not in target_dbs:
                    logger.error(
                        f"  MariaDB validation FAILED: "
                        f"database '{db}' missing on target"
                    )
                    passed = False
                    continue

                for table, source_count in tables.items():
                    exit_code, count_out = self._run(
                        jc, client,
                        f"mysql -u {db_user} -p'{db_password}' "
                        f"-h {ip} -N -e "
                        f"\"SELECT COUNT(*) FROM `{db}`.`{table}`\""
                    )
                    if exit_code != 0:
                        logger.error(
                            f"  FAILED: cannot query {db}.{table}"
                        )
                        passed = False
                        continue

                    try:
                        target_count = int(count_out.strip())
                    except ValueError:
                        target_count = -1

                    if target_count == source_count:
                        logger.info(
                            f"  {db}.{table}: {target_count} rows ✓"
                        )
                    else:
                        logger.error(
                            f"  FAILED: {db}.{table} "
                            f"source={source_count} target={target_count}"
                        )
                        passed = False

        if passed:
            logger.info("MariaDB validation PASSED")
        else:
            logger.error("MariaDB validation FAILED")
        return passed

    # -----------------------------------------------------------------------
    # Apache
    # -----------------------------------------------------------------------

    def validate_apache(self, ip: str, private_key_path: str) -> bool:
        logger.info(f"Validating Apache on {ip}...")
        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            exit_code, output = self._run(
                jc, client,
                "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1"
            )
        if exit_code == 0 and output.strip() in ("200", "301", "302"):
            logger.info(f"Apache validation PASSED (HTTP {output.strip()})")
            return True
        logger.error(f"Apache validation FAILED (HTTP {output.strip()})")
        return False

    # -----------------------------------------------------------------------
    # Backup service
    # -----------------------------------------------------------------------

    def validate_backup(self, ip: str, private_key_path: str,
                         mariadb_ip: str, db_user: str,
                         db_password: str) -> bool:
        logger.info(f"Validating Backup service on {ip}...")
        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)

            ec_cron, crontab = self._run(
                jc, client, "sudo crontab -l 2>/dev/null"
            )
            ec_dump, _ = self._run(jc, client, "which mysqldump")
            ec_conn, _ = self._run(
                jc, client,
                f"mysqldump -u {db_user} -p'{db_password}' "
                f"-h {mariadb_ip} --no-data 2>/dev/null | head -1"
            )

        cron_ok = ec_cron == 0 and len(crontab) > 0
        dump_ok = ec_dump == 0
        conn_ok = ec_conn == 0

        if cron_ok and dump_ok and conn_ok:
            logger.info("Backup validation PASSED")
            return True

        if not cron_ok:
            logger.error("  Backup: crontab missing or empty")
        if not dump_ok:
            logger.error("  Backup: mysqldump not installed")
        if not conn_ok:
            logger.error(
                f"  Backup: cannot reach MariaDB at {mariadb_ip}"
            )
        return False

    # -----------------------------------------------------------------------
    # NFS
    # -----------------------------------------------------------------------

    def validate_nfs(self, ip: str, private_key_path: str) -> bool:
        logger.info(f"Validating NFS on {ip}...")
        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            ec_svc, svc_out = self._run(
                jc, client,
                "sudo systemctl is-active nfs-kernel-server"
            )
            ec_exp, exports = self._run(
                jc, client, "sudo exportfs -v"
            )

        svc_ok = ec_svc == 0 and svc_out.strip() == "active"
        exp_ok = ec_exp == 0 and len(exports) > 0

        if svc_ok and exp_ok:
            logger.info("NFS validation PASSED")
            return True
        if not svc_ok:
            logger.error("  NFS: service not running")
        if not exp_ok:
            logger.error("  NFS: no exports found")
        return False

    # -----------------------------------------------------------------------
    # FTP
    # -----------------------------------------------------------------------

    def validate_ftp(self, ip: str, private_key_path: str,
                      server_type: str) -> bool:
        logger.info(f"Validating FTP ({server_type}) on {ip}...")
        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            ec_svc, svc_out = self._run(
                jc, client,
                f"sudo systemctl is-active {server_type}"
            )
            ec_port, port_out = self._run(
                jc, client, "ss -tlnp | grep ':21 '"
            )

        svc_ok = ec_svc == 0 and svc_out.strip() == "active"
        port_ok = ec_port == 0 and len(port_out) > 0

        if svc_ok and port_ok:
            logger.info("FTP validation PASSED")
            return True
        if not svc_ok:
            logger.error(f"  FTP: {server_type} not running")
        if not port_ok:
            logger.error("  FTP: port 21 not listening")
        return False

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _run(self, jc: JumpHostClient, client,
             command: str) -> tuple[int, str]:
        """Run a command and return (exit_code, stdout)."""
        _, stdout, _ = client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode().strip()
        return exit_code, output

    # -----------------------------------------------------------------------
    # Orchestration
    # -----------------------------------------------------------------------

    def validate_all(self, inventory: list, backup_paths: dict,
                     private_key_path: str, ports: dict,
                     db_user: str = "", db_password: str = "") -> dict:
        results = {}
        mariadb_ip = None

        for container in inventory:
            if container["service"] == "mariadb" and container["name"] in ports:
                mariadb_ip = ports[container["name"]].fixed_ips[0]["ip_address"]
                break

        for container in inventory:
            name = container["name"]
            service = container["service"]

            if name not in ports:
                continue

            ip = ports[name].fixed_ips[0]["ip_address"]

            try:
                if service == "mariadb":
                    results[name] = self.validate_mariadb(
                        ip, private_key_path, container,
                        db_user, db_password,
                        backup_paths.get(name, {})
                    )
                elif service == "apache":
                    results[name] = self.validate_apache(
                        ip, private_key_path
                    )
                elif service == "backup":
                    results[name] = self.validate_backup(
                        ip, private_key_path, mariadb_ip or "",
                        db_user, db_password
                    )
                elif service == "nfs":
                    results[name] = self.validate_nfs(
                        ip, private_key_path
                    )
                elif service == "ftp":
                    server_type = backup_paths.get(name, {}).get(
                        "server_type", "vsftpd"
                    )
                    results[name] = self.validate_ftp(
                        ip, private_key_path, server_type
                    )
            except Exception as e:
                logger.error(f"Validation error for {name}: {e}")
                results[name] = False

        logger.info("=" * 40)
        logger.info("MIGRATION RESULTS")
        logger.info("=" * 40)
        for name, passed in results.items():
            status = "PASSED ✓" if passed else "FAILED ✗"
            logger.info(f"  {name:15s} : {status}")
        logger.info("=" * 40)

        return results
