import os
import logging
import subprocess

from src.scanner import _lxc_attach


logger = logging.getLogger("migration")


def _lxc_to_file(name: str, output_path: str, *cmd_args: str) -> int:
    """Run a command inside container `name` and write stdout to output_path."""
    full_cmd = ["sudo", "lxc-attach", "-n", name, "--"] + list(cmd_args)
    with open(output_path, "wb") as f:
        result = subprocess.run(full_cmd, stdout=f, stderr=subprocess.DEVNULL)
    return result.returncode


class BackupManager:

    def __init__(self, config):
        self.config = config
        self.backup_dir = config["paths"]["backup_dir"]
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)

    def backup_mariadb(self, name):
        logger.info(f"Backing up MariaDB from {name}...")

        dump_path = os.path.join(self.backup_dir, "mariadb_dump.sql")
        databases = _lxc_attach(
            name, "mysql", "-u", "root", "-N", "-e", "SHOW DATABASES"
        )
        db_list = [
            db.strip() for db in databases.split("\n")
            if db.strip() and db.strip() not in
            {"information_schema", "mysql", "performance_schema", "sys"}
        ]

        if not db_list:
            raise Exception("No application databases found to backup")

        logger.info(f"Dumping databases: {' '.join(db_list)}")
        _lxc_to_file(
            name, dump_path,
            "mysqldump", "-u", "root",
            "--single-transaction", "--routines", "--triggers", "--events",
            "--databases", *db_list
        )

        if os.path.getsize(dump_path) == 0:
            raise Exception(
                "MariaDB dump is empty (0 bytes). "
                "Backup failed, aborting migration."
            )

        self._verify_file(dump_path, "MariaDB dump")
        logger.info(f"MariaDB backup complete: {self._file_size(dump_path)}")

        # Capture row counts at backup time so the validator can compare
        # against a frozen snapshot, not the live source which may keep writing.
        row_counts = {}
        for db in db_list:
            row_counts[db] = {}
            tables_out = _lxc_attach(
                name, "mysql", "-u", "root", "-N",
                "-e", f"SHOW TABLES IN `{db}`"
            )
            for table in tables_out.split("\n"):
                table = table.strip()
                if not table:
                    continue
                count_out = _lxc_attach(
                    name, "mysql", "-u", "root", "-N",
                    "-e", f"SELECT COUNT(*) FROM `{db}`.`{table}`"
                )
                try:
                    row_counts[db][table] = int(count_out.strip())
                except ValueError:
                    row_counts[db][table] = 0
        total_rows = sum(sum(t.values()) for t in row_counts.values())
        logger.info(f"  Captured {total_rows} rows across {len(db_list)} DBs")

        return {
            "dump": dump_path,
            "row_counts": row_counts,
        }

    def backup_apache(self, name):
        logger.info(f"Backing up Apache from {name}...")

        archive_path = os.path.join(self.backup_dir, "apache_backup.tar.gz")
        _lxc_to_file(
            name, archive_path,
            "tar", "czf", "-", "/var/www", "/etc/apache2", "/etc/php"
        )

        modules_path = os.path.join(self.backup_dir, "apache_modules.txt")
        _lxc_to_file(name, modules_path, "apache2ctl", "-M")

        self._verify_file(archive_path, "Apache archive")
        logger.info(f"Apache backup complete: {self._file_size(archive_path)}")

        return {
            "archive": archive_path,
            "modules": modules_path,
        }

    def backup_backup_service(self, name):
        logger.info(f"Backing up Backup service from {name}...")

        crontab_path = os.path.join(self.backup_dir, "backup_crontab.txt")
        _lxc_to_file(name, crontab_path, "crontab", "-l")

        crontab_content = ""
        if os.path.exists(crontab_path):
            with open(crontab_path, "r") as f:
                crontab_content = f.read()

        script_path = ""
        for line in crontab_content.split("\n"):
            if "mysqldump" in line or ".sh" in line:
                for part in line.split():
                    if "/" in part and part.endswith(".sh"):
                        script_path = part
                        break

        local_script_path = os.path.join(self.backup_dir, "backup_script.sh")
        if script_path:
            _lxc_to_file(name, local_script_path, "cat", script_path)

        logger.info("Backup service backup complete")

        return {
            "crontab": crontab_path,
            "script": local_script_path,
            "original_script_path": script_path,
        }

    def backup_nfs(self, name):
        logger.info(f"Backing up NFS from {name}...")

        exports_path = os.path.join(self.backup_dir, "nfs_exports.txt")
        _lxc_to_file(name, exports_path, "cat", "/etc/exports")

        shared_dirs = []
        if os.path.exists(exports_path):
            with open(exports_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split()
                        if parts:
                            shared_dirs.append(parts[0])

        data_archive_path = os.path.join(self.backup_dir, "nfs_data.tar.gz")
        if shared_dirs:
            _lxc_to_file(
                name, data_archive_path,
                "tar", "czf", "-", *shared_dirs
            )

        self._verify_file(exports_path, "NFS exports")
        logger.info("NFS backup complete")

        return {
            "exports": exports_path,
            "data_archive": data_archive_path,
            "shared_dirs": shared_dirs,
        }

    def backup_ftp(self, name):
        logger.info(f"Backing up FTP from {name}...")

        vsftpd_path = os.path.join(self.backup_dir, "vsftpd.conf")
        _lxc_to_file(name, vsftpd_path, "cat", "/etc/vsftpd.conf")

        server_type = "vsftpd"
        if os.path.getsize(vsftpd_path) == 0:
            server_type = "proftpd"
            proftpd_path = os.path.join(self.backup_dir, "proftpd.conf")
            _lxc_to_file(
                name, proftpd_path,
                "cat", "/etc/proftpd/proftpd.conf"
            )

        users_path = os.path.join(self.backup_dir, "ftp_users.txt")
        _lxc_to_file(name, users_path, "cat", "/etc/passwd")

        ftp_data_path = os.path.join(self.backup_dir, "ftp_data.tar.gz")
        _lxc_to_file(
            name, ftp_data_path,
            "tar", "czf", "-", "/srv/ftp", "/home"
        )

        logger.info(f"FTP backup complete (server: {server_type})")

        return {
            "config": vsftpd_path,
            "server_type": server_type,
            "users": users_path,
            "data_archive": ftp_data_path,
        }

    def _verify_file(self, path, description):
        if not os.path.exists(path):
            logger.error(f"{description} not found: {path}")
            raise Exception(f"Backup failed: {description}")
        if os.path.getsize(path) == 0:
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
