import os
import logging
from src.scanner import run_command


logger = logging.getLogger("migration")


class BackupManager:

    def __init__(self, config):
        self.config = config
        self.backup_dir = config["paths"]["backup_dir"]
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)

    def backup_mariadb(self, name):
        logger.info(f"Backing up MariaDB from {name}...")

        dump_path = os.path.join(self.backup_dir, "mariadb_dump.sql")
        databases = run_command(
            f"sudo lxc-attach -n {name} -- "
            f"mysql -u root -N -e 'SHOW DATABASES'"
        )
        db_list = [
            db.strip() for db in databases.split("\n")
            if db.strip() and db.strip() not in
            ["information_schema", "mysql", "performance_schema"]
            and db.strip() != "sys"
        ]
        db_names = " ".join(db_list)

        if not db_names:
            raise Exception(
                "No application databases found to backup"
            )

        logger.info(f"Dumping databases: {db_names}")

        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"bash -c 'mysqldump -u root --single-transaction "
            f"--routines --triggers --events "
            f"--databases {db_names}' > {dump_path}"
        )

        dump_size = os.path.getsize(dump_path)
        if dump_size == 0:
            raise Exception(
                "MariaDB dump is empty (0 bytes). "
                "Backup failed, aborting migration."
            )

        users_path = os.path.join(self.backup_dir, "mariadb_users.sql")
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"mysql -u root -N -e "
            f"\"SELECT CONCAT('CREATE USER IF NOT EXISTS ', "
            f"QUOTE(User), '@', QUOTE(Host), "
            f"' IDENTIFIED BY PASSWORD ', QUOTE(Password), ';') "
            f"FROM mysql.user WHERE User NOT IN "
            f"('root', 'mariadb.sys', 'mysql', '')\" "
            f"> {users_path}"
        )

        self._verify_file(dump_path, "MariaDB dump")
        logger.info(
            f"MariaDB backup complete: {self._file_size(dump_path)}"
        )

        # Capture row counts at backup time (after the dump) so the
        # validator can compare against a frozen snapshot — not the live
        # source which keeps writing during/after migration.
        row_counts = {}
        for db in db_list:
            row_counts[db] = {}
            tables_out = run_command(
                f"sudo lxc-attach -n {name} -- "
                f"mysql -u root -N -e 'SHOW TABLES IN `{db}`'"
            )
            for table in tables_out.split("\n"):
                table = table.strip()
                if not table:
                    continue
                count_out = run_command(
                    f"sudo lxc-attach -n {name} -- "
                    f"mysql -u root -N -e "
                    f"'SELECT COUNT(*) FROM `{db}`.`{table}`'"
                )
                try:
                    row_counts[db][table] = int(count_out.strip())
                except ValueError:
                    row_counts[db][table] = 0
        total_rows = sum(sum(t.values()) for t in row_counts.values())
        logger.info(f"  Captured {total_rows} rows across {len(db_list)} DBs")

        return {
            "dump": dump_path,
            "users": users_path,
            "row_counts": row_counts,
        }

    def backup_apache(self, name):
        logger.info(f"Backing up Apache from {name}...")

        archive_path = os.path.join(
            self.backup_dir, "apache_backup.tar.gz"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"tar czf - /var/www /etc/apache2 /etc/php "
            f"2>/dev/null > {archive_path}"
        )

        modules_path = os.path.join(
            self.backup_dir, "apache_modules.txt"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"apache2ctl -M 2>/dev/null > {modules_path}"
        )

        self._verify_file(archive_path, "Apache archive")
        logger.info(
            f"Apache backup complete: {self._file_size(archive_path)}"
        )

        return {
            "archive": archive_path,
            "modules": modules_path
        }

    def backup_backup_service(self, name):
        logger.info(f"Backing up Backup service from {name}...")

        crontab_path = os.path.join(
            self.backup_dir, "backup_crontab.txt"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"crontab -l 2>/dev/null > {crontab_path}"
        )

        crontab_content = ""
        if os.path.exists(crontab_path):
            with open(crontab_path, "r") as f:
                crontab_content = f.read()

        script_path = ""
        for line in crontab_content.split("\n"):
            if "mysqldump" in line or ".sh" in line:
                parts = line.split()
                for part in parts:
                    if "/" in part and ".sh" in part:
                        script_path = part
                        break

        local_script_path = os.path.join(
            self.backup_dir, "backup_script.sh"
        )
        if script_path:
            run_command(
                f"sudo lxc-attach -n {name} -- "
                f"cat {script_path} > {local_script_path}"
            )

        logger.info("Backup service backup complete")

        return {
            "crontab": crontab_path,
            "script": local_script_path,
            "original_script_path": script_path
        }

    def backup_nfs(self, name):
        logger.info(f"Backing up NFS from {name}...")

        exports_path = os.path.join(
            self.backup_dir, "nfs_exports.txt"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"cat /etc/exports > {exports_path}"
        )

        shared_dirs = []
        if os.path.exists(exports_path):
            with open(exports_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split()
                        if parts:
                            shared_dirs.append(parts[0])

        data_archive_path = os.path.join(
            self.backup_dir, "nfs_data.tar.gz"
        )
        if shared_dirs:
            dirs_string = " ".join(shared_dirs)
            run_command(
                f"sudo lxc-attach -n {name} -- "
                f"tar czf - {dirs_string} "
                f"2>/dev/null > {data_archive_path}"
            )

        self._verify_file(exports_path, "NFS exports")
        logger.info("NFS backup complete")

        return {
            "exports": exports_path,
            "data_archive": data_archive_path,
            "shared_dirs": shared_dirs
        }

    def backup_ftp(self, name):
        logger.info(f"Backing up FTP from {name}...")

        vsftpd_path = os.path.join(
            self.backup_dir, "vsftpd.conf"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"cat /etc/vsftpd.conf 2>/dev/null > {vsftpd_path}"
        )

        server_type = "vsftpd"
        if os.path.getsize(vsftpd_path) == 0:
            server_type = "proftpd"
            proftpd_path = os.path.join(
                self.backup_dir, "proftpd.conf"
            )
            run_command(
                f"sudo lxc-attach -n {name} -- "
                f"cat /etc/proftpd/proftpd.conf "
                f"2>/dev/null > {proftpd_path}"
            )

        users_path = os.path.join(
            self.backup_dir, "ftp_users.txt"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"cat /etc/passwd > {users_path}"
        )

        ftp_data_path = os.path.join(
            self.backup_dir, "ftp_data.tar.gz"
        )
        run_command(
            f"sudo lxc-attach -n {name} -- "
            f"tar czf - /srv/ftp /home "
            f"2>/dev/null > {ftp_data_path}"
        )

        logger.info(f"FTP backup complete (server: {server_type})")

        return {
            "config": vsftpd_path,
            "server_type": server_type,
            "users": users_path,
            "data_archive": ftp_data_path
        }

    def _verify_file(self, path, description):
        if not os.path.exists(path):
            logger.error(f"{description} not found: {path}")
            raise Exception(f"Backup failed: {description}")
        size = os.path.getsize(path)
        if size == 0:
            logger.warning(f"{description} is empty: {path}")

    def _file_size(self, path):
        size = os.path.getsize(path)
        if size < 1024:
            return f"{size}B"
        elif size < 1024 * 1024:
            return f"{size // 1024}KB"
        else:
            return f"{size // (1024 * 1024)}MB"

    def backup_all(self, inventory):
        backup_paths = {}

        for container in inventory:
            name = container["name"]
            service = container["service"]

            if service == "mariadb":
                backup_paths[name] = self.backup_mariadb(name)
            elif service == "apache":
                backup_paths[name] = self.backup_apache(name)
            elif service == "backup":
                backup_paths[name] = self.backup_backup_service(name)
            elif service == "nfs":
                backup_paths[name] = self.backup_nfs(name)
            elif service == "ftp":
                backup_paths[name] = self.backup_ftp(name)
            else:
                logger.warning(
                    f"Unknown service {service} on {name}, skipping"
                )

        logger.info(
            f"All backups complete: {len(backup_paths)} services"
        )
        return backup_paths
