"""
Scanner — découverte de l'infrastructure LXC source.

Nouveautés v2 :
- detect_os() : codename Ubuntu/Debian + chemin config MariaDB
- get_mariadb_app_users() : users applicatifs réels (pas root/system)
- get_disk_usage_mb() : retourne un entier MB pour le sizing automatique
- get_ram_usage_mb()  : idem
"""

import re
import subprocess
import logging

logger = logging.getLogger("migration")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lxc_attach(name: str, *cmd_args: str) -> str:
    """
    Run a command inside container `name` via lxc-attach.
    Uses a list (no shell=True) to prevent command injection.
    """
    full_cmd = ["sudo", "lxc-attach", "-n", name, "--"] + list(cmd_args)
    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True
    )
    return result.stdout.strip()


def _lxc_host(*cmd_args: str) -> str:
    """Run a host-level lxc command (lxc-ls, lxc-info…)."""
    result = subprocess.run(
        ["sudo"] + list(cmd_args),
        capture_output=True,
        text=True
    )
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Container enumeration
# ---------------------------------------------------------------------------

def list_containers() -> list[str]:
    output = _lxc_host("lxc-ls", "--active")
    return output.split() if output else []


def get_container_ip(name: str) -> str:
    return _lxc_host("lxc-info", "-n", name, "-iH")


# ---------------------------------------------------------------------------
# OS / environment detection (NEW in v2)
# ---------------------------------------------------------------------------

def detect_os(name: str) -> dict:
    """
    Return OS details for the container:
      codename  : 'jammy', 'bookworm', etc.
      id        : 'ubuntu', 'debian', etc.
      version   : '22.04', '12', etc.
      image_url : cloud image URL to use for the OpenStack instance
      mariadb_cnf_path : path to MariaDB bind-address config
    """
    codename = _lxc_attach(name, "lsb_release", "-cs").strip() or "jammy"
    distro_id = _lxc_attach(name, "lsb_release", "-is").lower().strip() or "ubuntu"
    version = _lxc_attach(name, "lsb_release", "-rs").strip() or "22.04"

    # Map codename → cloud image URL
    _image_map = {
        "jammy":   "https://cloud-images.ubuntu.com/jammy/current/jammy-server-cloudimg-amd64.img",
        "noble":   "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
        "focal":   "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
        "bookworm":"https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-genericcloud-amd64.qcow2",
        "bullseye":"https://cloud.debian.org/images/cloud/bullseye/latest/debian-11-genericcloud-amd64.qcow2",
    }
    image_url = _image_map.get(
        codename,
        f"https://cloud-images.ubuntu.com/{codename}/current/{codename}-server-cloudimg-amd64.img"
    )

    # SSH user depends on distro
    ssh_user_map = {"ubuntu": "ubuntu", "debian": "debian"}
    default_ssh_user = ssh_user_map.get(distro_id, "ubuntu")

    # MariaDB config path
    mariadb_cnf = _detect_mariadb_cnf_path(name)

    return {
        "codename": codename,
        "id": distro_id,
        "version": version,
        "image_url": image_url,
        "default_ssh_user": default_ssh_user,
        "mariadb_cnf_path": mariadb_cnf,
    }


def _detect_mariadb_cnf_path(name: str) -> str:
    """Find the MariaDB config file that contains bind-address."""
    candidates = [
        "/etc/mysql/mariadb.conf.d/50-server.cnf",
        "/etc/mysql/my.cnf",
        "/etc/mysql/mysql.conf.d/mysqld.cnf",
        "/etc/mysql/mariadb.cnf",
    ]
    for path in candidates:
        result = subprocess.run(
            ["sudo", "lxc-attach", "-n", name, "--", "test", "-f", path],
            capture_output=True
        )
        if result.returncode == 0:
            return path
    return "/etc/mysql/my.cnf"


# ---------------------------------------------------------------------------
# Resource usage (returns integers for auto-sizing)
# ---------------------------------------------------------------------------

def get_disk_usage_mb(name: str) -> int:
    """Return disk used by container root in MB (integer)."""
    out = _lxc_attach(name, "df", "--output=used", "-BM", "/")
    for line in out.split("\n"):
        line = line.strip().rstrip("M")
        if line.isdigit():
            return int(line)
    return 1024  # default 1 GB


def get_disk_usage(name: str) -> str:
    """Human-readable string (kept for display)."""
    out = _lxc_attach(name, "df", "-h", "/", "--output=used")
    lines = out.split("\n")
    return lines[1].strip() if len(lines) >= 2 else "unknown"


def get_ram_usage_mb(name: str) -> dict:
    """Return RAM total/used in MB as integers."""
    out = _lxc_attach(name, "free", "-m")
    for line in out.split("\n"):
        if line.startswith("Mem:"):
            parts = line.split()
            return {
                "total": int(parts[1]) if parts[1].isdigit() else 512,
                "used":  int(parts[2]) if parts[2].isdigit() else 256,
            }
    return {"total": 512, "used": 256}


def get_ram_usage(name: str) -> dict:
    """String version (kept for display)."""
    out = _lxc_attach(name, "free", "-m")
    for line in out.split("\n"):
        if "Mem:" in line:
            parts = line.split()
            return {"total": parts[1], "used": parts[2]}
    return {"total": "unknown", "used": "unknown"}


# ---------------------------------------------------------------------------
# Network / ports
# ---------------------------------------------------------------------------

def get_open_ports(name: str) -> list[int]:
    output = _lxc_attach(name, "ss", "-tlnp")
    ports = []
    for line in output.split("\n"):
        if "LISTEN" in line:
            match = re.search(r":(\d+)\s", line)
            if match:
                port = int(match.group(1))
                if port not in ports:
                    ports.append(port)
    return ports


# ---------------------------------------------------------------------------
# Package list
# ---------------------------------------------------------------------------

def get_installed_packages(name: str) -> list[str]:
    output = _lxc_attach(name, "dpkg", "-l")
    packages = []
    for line in output.split("\n"):
        if line.startswith("ii"):
            parts = line.split()
            if len(parts) >= 2:
                packages.append(parts[1])
    return packages


# ---------------------------------------------------------------------------
# Service detection
# ---------------------------------------------------------------------------

def detect_service(name: str, ports: list[int]) -> str:
    if 3306 in ports:
        return "mariadb"
    if 80 in ports:
        return "apache"
    if 21 in ports:
        return "ftp"
    if 2049 in ports:
        return "nfs"

    crontab = _lxc_attach(name, "bash", "-c", "crontab -l 2>/dev/null")
    if "mysqldump" in crontab:
        return "backup"
    if "backup" in crontab or ".sh" in crontab:
        for line in crontab.split("\n"):
            line = line.strip()
            if line and not line.startswith("#"):
                for part in line.split():
                    if "/" in part and part.endswith(".sh"):
                        content = _lxc_attach(name, "cat", part)
                        if "mysqldump" in content or "backup" in content:
                            return "backup"
    return "unknown"


# ---------------------------------------------------------------------------
# Service-specific details
# ---------------------------------------------------------------------------

def get_mariadb_details(name: str) -> dict:
    databases = _lxc_attach(
        name, "mysql", "-u", "root", "-e", "SHOW DATABASES"
    )
    users = _lxc_attach(
        name, "mysql", "-u", "root",
        "-e", "SELECT User,Host FROM mysql.user"
    )
    return {
        "databases": databases.split("\n")[1:],
        "users": users.split("\n")[1:],
    }


def get_mariadb_app_users(name: str) -> list[dict]:
    """
    Return non-system MariaDB users (those who own at least one database).
    Each entry: {"user": "...", "host": "..."}

    These users will be recreated on the target instance from the SQL dump,
    so no password needs to be discovered here.
    """
    system_users = {"root", "mariadb.sys", "mysql", "", "debian-sys-maint"}
    out = _lxc_attach(
        name, "mysql", "-u", "root", "-N",
        "-e", "SELECT User, Host FROM mysql.user"
    )
    app_users = []
    for line in out.split("\n"):
        parts = line.strip().split("\t")
        if len(parts) == 2:
            user, host = parts[0].strip(), parts[1].strip()
            if user and user not in system_users:
                app_users.append({"user": user, "host": host})
    return app_users


def get_mariadb_app_databases(name: str) -> list[str]:
    """Return user databases (excluding system schemas)."""
    system_dbs = {
        "information_schema", "mysql",
        "performance_schema", "sys"
    }
    out = _lxc_attach(
        name, "mysql", "-u", "root", "-N", "-e", "SHOW DATABASES"
    )
    return [
        db.strip() for db in out.split("\n")
        if db.strip() and db.strip() not in system_dbs
    ]


def get_apache_details(name: str) -> dict:
    vhosts = _lxc_attach(name, "ls", "/etc/apache2/sites-enabled/")
    modules = _lxc_attach(name, "apache2ctl", "-M")
    www_content = _lxc_attach(name, "ls", "/var/www/")
    return {
        "vhosts": vhosts.split(),
        "modules": modules.split("\n"),
        "www_content": www_content.split(),
    }


def get_backup_details(name: str) -> dict:
    crontab = _lxc_attach(name, "bash", "-c", "crontab -l 2>/dev/null")
    script_path = ""
    for line in crontab.split("\n"):
        if "mysqldump" in line or ".sh" in line:
            for part in line.split():
                if "/" in part and part.endswith(".sh"):
                    script_path = part
                    break
    script_content = ""
    if script_path:
        script_content = _lxc_attach(name, "cat", script_path)
    return {
        "crontab": crontab,
        "script_path": script_path,
        "script_content": script_content,
    }


def get_nfs_details(name: str) -> dict:
    exports = _lxc_attach(name, "cat", "/etc/exports")
    shared_dirs = []
    for line in exports.split("\n"):
        line = line.strip()
        if line and not line.startswith("#"):
            parts = line.split()
            if parts:
                shared_dirs.append(parts[0])
    return {"exports": exports, "shared_dirs": shared_dirs}


def get_ftp_details(name: str) -> dict:
    vsftpd_conf = _lxc_attach(
        name, "bash", "-c", "cat /etc/vsftpd.conf 2>/dev/null"
    )
    proftpd_conf = ""
    if not vsftpd_conf:
        proftpd_conf = _lxc_attach(
            name, "bash", "-c",
            "cat /etc/proftpd/proftpd.conf 2>/dev/null"
        )
    ftp_users = _lxc_attach(
        name, "bash", "-c", "cat /etc/ftpusers 2>/dev/null"
    )
    return {
        "vsftpd_conf": vsftpd_conf,
        "proftpd_conf": proftpd_conf,
        "ftp_users": ftp_users,
        "server": "vsftpd" if vsftpd_conf else "proftpd",
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scan_all() -> list[dict]:
    inventory = []
    containers = list_containers()

    for name in containers:
        ip = get_container_ip(name)
        ports = get_open_ports(name)
        service = detect_service(name, ports)
        disk_str = get_disk_usage(name)
        disk_mb = get_disk_usage_mb(name)
        ram = get_ram_usage(name)
        ram_mb = get_ram_usage_mb(name)
        packages = get_installed_packages(name)
        os_info = detect_os(name)

        container_info = {
            "name":      name,
            "ip":        ip,
            "ports":     ports,
            "service":   service,
            "disk_usage": disk_str,
            "disk_mb":   disk_mb,
            "ram":       ram,
            "ram_mb":    ram_mb,
            "packages":  packages,
            "os":        os_info,
            "details":   {},
        }

        if service == "mariadb":
            container_info["details"] = get_mariadb_details(name)
            container_info["app_users"] = get_mariadb_app_users(name)
            container_info["app_databases"] = get_mariadb_app_databases(name)
        elif service == "apache":
            container_info["details"] = get_apache_details(name)
        elif service == "backup":
            container_info["details"] = get_backup_details(name)
        elif service == "nfs":
            container_info["details"] = get_nfs_details(name)
        elif service == "ftp":
            container_info["details"] = get_ftp_details(name)

        inventory.append(container_info)

    return inventory
