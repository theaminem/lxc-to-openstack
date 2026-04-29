"""
Transfer — envoi des fichiers de backup vers les instances OpenStack.

Améliorations v2 :
- Utilise JumpHostClient (plus de duplication SSH, _is_tenant_ip fixé)
- put_file() via JumpHostClient gère les deux modes (provider + tenant)
"""

import logging
import os

from src.jump_client import JumpHostClient

logger = logging.getLogger("migration")


class Transfer:

    def __init__(self, config: dict):
        self.config = config

    # -----------------------------------------------------------------------
    # Core upload
    # -----------------------------------------------------------------------

    def _upload(self, ip: str, private_key_path: str,
                local_path: str, remote_path: str, mode: str = ""):
        if not os.path.exists(local_path):
            logger.warning(f"File not found, skipping: {local_path}")
            return False

        size = self._fmt(os.path.getsize(local_path))
        logger.info(
            f"  Uploading {os.path.basename(local_path)} → "
            f"{ip}:{remote_path} ({size})"
        )

        with JumpHostClient(self.config) as jc:
            client = jc.connect(ip, private_key_path)
            remote_dir = remote_path.rsplit("/", 1)[0]
            if remote_dir:
                jc.run_soft(client, f"mkdir -p {remote_dir}")
            jc.put_file(client, local_path, remote_path)
            if mode:
                jc.run_soft(client, f"chmod {mode} {remote_path}")

        logger.info(f"  Done: {remote_path}")
        return True

    # -----------------------------------------------------------------------
    # Per-service transfer
    # -----------------------------------------------------------------------

    def transfer_mariadb(self, ip: str, private_key_path: str,
                          backup_paths: dict):
        logger.info(f"Transferring MariaDB data to {ip}...")
        self._upload(ip, private_key_path,
                     backup_paths["dump"], "/tmp/mariadb_dump.sql", mode="600")
        logger.info("MariaDB transfer complete")

    def transfer_apache(self, ip: str, private_key_path: str,
                         backup_paths: dict):
        logger.info(f"Transferring Apache data to {ip}...")
        self._upload(ip, private_key_path,
                     backup_paths["archive"], "/tmp/apache_backup.tar.gz")
        self._upload(ip, private_key_path,
                     backup_paths["modules"], "/tmp/apache_modules.txt")
        logger.info("Apache transfer complete")

    def transfer_backup(self, ip: str, private_key_path: str,
                         backup_paths: dict):
        logger.info(f"Transferring Backup data to {ip}...")
        self._upload(ip, private_key_path,
                     backup_paths["crontab"], "/tmp/backup_crontab.txt")
        if backup_paths.get("script"):
            self._upload(ip, private_key_path,
                         backup_paths["script"], "/tmp/backup_script.sh")
        logger.info("Backup transfer complete")

    def transfer_nfs(self, ip: str, private_key_path: str,
                      backup_paths: dict):
        logger.info(f"Transferring NFS data to {ip}...")
        self._upload(ip, private_key_path,
                     backup_paths["exports"], "/tmp/nfs_exports.txt")
        if backup_paths.get("data_archive"):
            self._upload(ip, private_key_path,
                         backup_paths["data_archive"], "/tmp/nfs_data.tar.gz")
        logger.info("NFS transfer complete")

    def transfer_ftp(self, ip: str, private_key_path: str,
                      backup_paths: dict):
        logger.info(f"Transferring FTP data to {ip}...")
        self._upload(ip, private_key_path,
                     backup_paths["config"], "/tmp/ftp_config.conf")
        self._upload(ip, private_key_path,
                     backup_paths["users"], "/tmp/ftp_users.txt")
        if backup_paths.get("data_archive"):
            self._upload(ip, private_key_path,
                         backup_paths["data_archive"], "/tmp/ftp_data.tar.gz")
        logger.info("FTP transfer complete")

    # -----------------------------------------------------------------------
    # Orchestration
    # -----------------------------------------------------------------------

    def transfer_all(self, inventory: list, backup_paths: dict,
                     private_key_path: str, ports: dict):
        for container in inventory:
            name = container["name"]
            service = container["service"]

            if name not in backup_paths:
                logger.warning(f"No backup for {name}, skipping transfer")
                continue
            if name not in ports:
                continue

            ip = ports[name].fixed_ips[0]["ip_address"]
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

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _fmt(size: int) -> str:
        if size < 1024:
            return f"{size}B"
        elif size < 1024 * 1024:
            return f"{size // 1024}KB"
        else:
            return f"{size // (1024 * 1024)}MB"
