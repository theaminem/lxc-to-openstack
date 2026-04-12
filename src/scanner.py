import subprocess
import re


def run_command(command):
    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True
    )
    return result.stdout.strip()

def list_containers():
    output = run_command("sudo lxc-ls --active")
    if not output:
        return []
    containers = output.split()
    return containers

def get_container_ip(name):
    output = run_command(f"sudo lxc-info -n {name} -iH")
    return output


def get_open_ports(name):
    output = run_command(
        f"sudo lxc-attach -n {name} -- ss -tlnp"
    )
    ports = []
    for line in output.split("\n"):
        if "LISTEN" in line:
            match = re.search(r":(\d+)\s", line)
            if match:
                port = int(match.group(1))
                if port not in ports:
                    ports.append(port)
    return ports


def get_disk_usage(name):
    output = run_command(
        f"sudo lxc-attach -n {name} -- df -h / --output=used"
    )
    lines = output.split("\n")
    if len(lines) >= 2:
        return lines[1].strip()
    return "unknown"


def get_ram_usage(name):
    output = run_command(
        f"sudo lxc-attach -n {name} -- free -m"
    )
    for line in output.split("\n"):
        if "Mem:" in line:
            parts = line.split()
            return {"total": parts[1], "used": parts[2]}
    return {"total": "unknown", "used": "unknown"}


def get_installed_packages(name):
    output = run_command(
        f"sudo lxc-attach -n {name} -- dpkg -l"
    )
    packages = []
    for line in output.split("\n"):
        if line.startswith("ii"):
            parts = line.split()
            packages.append(parts[1])
    return packages


def detect_service(name, ports):
    if 3306 in ports:
        return "mariadb"
    if 80 in ports:
        return "apache"
    if 21 in ports:
        return "ftp"
    if 2049 in ports:
        return "nfs"
    crontab = run_command(
        f"sudo lxc-attach -n {name} -- crontab -l 2>/dev/null"
    )
    if "mysqldump" in crontab:
        return "backup"
    return "unknown"


def get_mariadb_details(name):
    databases = run_command(
        f"sudo lxc-attach -n {name} -- mysql -u root -e 'SHOW DATABASES'" 
    )
    users = run_command(
        f"sudo lxc-attach -n {name} -- mysql -u root -e \"SELECT User,Host FROM mysql.user\""
    )
    return {
        "databases": databases.split("\n")[1:],
        "users": users.split("\n")[1:]
    }


def get_apache_details(name):
    vhosts = run_command(
        f"sudo lxc-attach -n {name} -- ls /etc/apache2/sites-enabled/"
    )
    modules = run_command(
        f"sudo lxc-attach -n {name} -- apache2ctl -M 2>/dev/null"
    )
    www_content = run_command(
        f"sudo lxc-attach -n {name} -- ls /var/www/"
    )
    return {
        "vhosts": vhosts.split(),
        "modules": modules.split("\n"),
        "www_content": www_content.split()
    }


def get_backup_details(name):
    crontab = run_command(
        f"sudo lxc-attach -n {name} -- crontab -l 2>/dev/null"
    )
    script_path = ""
    for line in crontab.split("\n"):
        if "mysqldump" in line or ".sh" in line:
            parts = line.split()
            for part in parts:
                if "/" in part and ".sh" in part:
                    script_path = part
                    break
    script_content = ""
    if script_path:
        script_content = run_command(
            f"sudo lxc-attach -n {name} -- cat {script_path}"
        )
    return {
        "crontab": crontab,
        "script_path": script_path,
        "script_content": script_content
    }


def get_nfs_details(name):
    exports = run_command(
        f"sudo lxc-attach -n {name} -- cat /etc/exports"
    )
    shared_dirs = []
    for line in exports.split("\n"):
        line = line.strip()
        if line and not line.startswith("#"):
            parts = line.split()
            if parts:
                shared_dirs.append(parts[0])
    return {
        "exports": exports,
        "shared_dirs": shared_dirs
    }


def get_ftp_details(name):
    vsftpd_conf = run_command(
        f"sudo lxc-attach -n {name} -- cat /etc/vsftpd.conf 2>/dev/null"
    )
    proftpd_conf = ""
    if not vsftpd_conf:
        proftpd_conf = run_command(
            f"sudo lxc-attach -n {name} -- cat /etc/proftpd/proftpd.conf 2>/dev/null"
        )
    ftp_users = run_command(
        f"sudo lxc-attach -n {name} -- cat /etc/ftpusers 2>/dev/null"
    )
    return {
        "vsftpd_conf": vsftpd_conf,
        "proftpd_conf": proftpd_conf,
        "ftp_users": ftp_users,
        "server": "vsftpd" if vsftpd_conf else "proftpd"
    }


def scan_all():
    inventory = []
    containers = list_containers()

    for name in containers:
        ip = get_container_ip(name)
        ports = get_open_ports(name)
        service = detect_service(name, ports)
        disk = get_disk_usage(name)
        ram = get_ram_usage(name)
        packages = get_installed_packages(name)

        container_info = {
            "name": name,
            "ip": ip,
            "ports": ports,
            "service": service,
            "disk_usage": disk,
            "ram": ram,
            "packages": packages,
            "details": {}
        }

        if service == "mariadb":
            container_info["details"] = get_mariadb_details(name)
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
