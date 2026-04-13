import sys
import getpass
from src.config_loader import load_config
from src.logger import setup_logger
from src.scanner import scan_all, run_command
from src.network_manager import NetworkManager
from src.validator import Validator


def get_source_data():
    inventory = scan_all()

    source_data = {}
    for container in inventory:
        name = container["name"]
        service = container["service"]

        if service == "mariadb":
            count = run_command(
                f"sudo lxc-attach -n {name} -- "
                f"mysql -u root -N -e "
                f"\"SELECT COUNT(*) FROM app_db.users\""
            )
            tables = run_command(
                f"sudo lxc-attach -n {name} -- "
                f"mysql -u root -N -e "
                f"\"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_schema='app_db'\""
            )
            users = run_command(
                f"sudo lxc-attach -n {name} -- "
                f"mysql -u root -N -e "
                f"\"SELECT COUNT(*) FROM mysql.user\""
            )
            source_data["mariadb"] = {
                "row_count": count.strip(),
                "table_count": tables.strip(),
                "user_count": users.strip()
            }

        elif service == "apache":
            html = run_command(
                f"sudo lxc-attach -n {name} -- "
                f"curl -s http://127.0.0.1"
            )
            source_data["apache"] = {
                "html_length": len(html)
            }

    return inventory, source_data


def test_mariadb_integrity(validator, ip, key_path,
                            source_data, results):
    print("\n--- Test: MariaDB data integrity ---")

    client, jump = validator._get_ssh_client(ip, key_path)

    exit_code, row_count = validator._run_remote(
        client,
        "sudo mysql -u root -N -e "
        "\"SELECT COUNT(*) FROM app_db.users\""
    )
    source_rows = source_data["mariadb"]["row_count"]
    target_rows = row_count.strip()
    if exit_code == 0 and target_rows == source_rows:
        print(f"  Row count: PASS ({target_rows} rows)")
        results["MariaDB row count"] = "PASS"
    else:
        print(f"  Row count: FAIL (source={source_rows}, "
              f"target={target_rows})")
        results["MariaDB row count"] = "FAIL"

    exit_code, table_count = validator._run_remote(
        client,
        "sudo mysql -u root -N -e "
        "\"SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='app_db'\""
    )
    source_tables = source_data["mariadb"]["table_count"]
    target_tables = table_count.strip()
    if exit_code == 0 and target_tables == source_tables:
        print(f"  Table count: PASS ({target_tables} tables)")
        results["MariaDB table count"] = "PASS"
    else:
        print(f"  Table count: FAIL (source={source_tables}, "
              f"target={target_tables})")
        results["MariaDB table count"] = "FAIL"

    exit_code, user_count = validator._run_remote(
        client,
        "sudo mysql -u root -N -e "
        "\"SELECT COUNT(*) FROM mysql.user\""
    )
    source_users = source_data["mariadb"]["user_count"]
    target_users = user_count.strip()
    if exit_code == 0 and target_users == source_users:
        print(f"  User count: PASS ({target_users} users)")
        results["MariaDB user count"] = "PASS"
    else:
        print(f"  User count: FAIL (source={source_users}, "
              f"target={target_users})")
        results["MariaDB user count"] = "FAIL"

    exit_code, insert_test = validator._run_remote(
        client,
        "sudo mysql -u root -e "
        "\"INSERT INTO app_db.users (name) VALUES ('test_migration'); "
        "SELECT ROW_COUNT(); "
        "DELETE FROM app_db.users WHERE name='test_migration';\""
    )
    if exit_code == 0:
        print("  Write test: PASS (INSERT + DELETE OK)")
        results["MariaDB write test"] = "PASS"
    else:
        print("  Write test: FAIL")
        results["MariaDB write test"] = "FAIL"

    client.close()
    jump.close()


def test_apache_service(validator, ip, key_path, results):
    print("\n--- Test: Apache HTTP service ---")

    client, jump = validator._get_ssh_client(ip, key_path)

    exit_code, http_code = validator._run_remote(
        client,
        "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1"
    )
    if exit_code == 0 and http_code.strip() == "200":
        print("  HTTP status: PASS (200)")
        results["Apache HTTP status"] = "PASS"
    else:
        print(f"  HTTP status: FAIL ({http_code})")
        results["Apache HTTP status"] = "FAIL"

    exit_code, html = validator._run_remote(
        client,
        "curl -s http://127.0.0.1"
    )
    if exit_code == 0 and len(html) > 0:
        print(f"  Content check: PASS ({len(html)} bytes)")
        results["Apache content"] = "PASS"
    else:
        print("  Content check: FAIL (empty response)")
        results["Apache content"] = "FAIL"

    client.close()
    jump.close()


def test_backup_service(validator, ip, key_path,
                         mariadb_ip, results):
    print("\n--- Test: Backup service ---")

    client, jump = validator._get_ssh_client(ip, key_path)

    exit_code, crontab = validator._run_remote(
        client,
        "crontab -l 2>/dev/null"
    )
    if exit_code == 0 and len(crontab) > 0:
        print("  Crontab: PASS")
        results["Backup crontab"] = "PASS"
    else:
        print("  Crontab: FAIL")
        results["Backup crontab"] = "FAIL"

    exit_code, dump_path = validator._run_remote(
        client,
        "which mysqldump"
    )
    if exit_code == 0:
        print("  mysqldump installed: PASS")
        results["Backup mysqldump"] = "PASS"
    else:
        print("  mysqldump installed: FAIL")
        results["Backup mysqldump"] = "FAIL"

    exit_code, dump_test = validator._run_remote(
        client,
        f"mysqldump -u appuser -ppass123 -h {mariadb_ip} "
        f"--no-data app_db 2>/dev/null | head -5"
    )
    if exit_code == 0 and len(dump_test) > 0:
        print("  Backup execution: PASS")
        results["Backup execution"] = "PASS"
    else:
        print("  Backup execution: FAIL")
        results["Backup execution"] = "FAIL"

    client.close()
    jump.close()


def test_nfs_service(validator, ip, key_path, results):
    print("\n--- Test: NFS service ---")

    client, jump = validator._get_ssh_client(ip, key_path)

    exit_code, status = validator._run_remote(
        client,
        "sudo systemctl is-active nfs-kernel-server"
    )
    if exit_code == 0 and status.strip() == "active":
        print("  Service status: PASS (active)")
        results["NFS service"] = "PASS"
    else:
        print("  Service status: FAIL")
        results["NFS service"] = "FAIL"

    exit_code, exports = validator._run_remote(
        client,
        "sudo exportfs -v"
    )
    if exit_code == 0 and len(exports) > 0:
        print("  Exports: PASS")
        results["NFS exports"] = "PASS"
    else:
        print("  Exports: FAIL")
        results["NFS exports"] = "FAIL"

    client.close()
    jump.close()


def test_ftp_service(validator, ip, key_path, server_type,
                      results):
    print("\n--- Test: FTP service ---")

    client, jump = validator._get_ssh_client(ip, key_path)

    exit_code, status = validator._run_remote(
        client,
        f"sudo systemctl is-active {server_type}"
    )
    if exit_code == 0 and status.strip() == "active":
        print(f"  Service status: PASS ({server_type} active)")
        results["FTP service"] = "PASS"
    else:
        print(f"  Service status: FAIL")
        results["FTP service"] = "FAIL"

    exit_code, port_check = validator._run_remote(
        client,
        "ss -tlnp | grep ':21 '"
    )
    if exit_code == 0 and len(port_check) > 0:
        print("  Port 21: PASS (listening)")
        results["FTP port 21"] = "PASS"
    else:
        print("  Port 21: FAIL")
        results["FTP port 21"] = "FAIL"

    client.close()
    jump.close()


def test_connectivity(validator, inventory, key_path, results):
    print("\n--- Test: Network connectivity ---")

    mariadb_ip = None
    apache_ip = None
    for c in inventory:
        if c["service"] == "mariadb":
            mariadb_ip = c["ip"]
        if c["service"] == "apache":
            apache_ip = c["ip"]

    if apache_ip and mariadb_ip:
        client, jump = validator._get_ssh_client(
            apache_ip, key_path
        )
        exit_code, output = validator._run_remote(
            client,
            f"nc -z -w 3 {mariadb_ip} 3306 && echo OK || echo FAIL"
        )
        if "OK" in output:
            print(f"  Apache -> MariaDB (3306): PASS")
            results["Connectivity apache->mariadb"] = "PASS"
        else:
            print(f"  Apache -> MariaDB (3306): FAIL")
            results["Connectivity apache->mariadb"] = "FAIL"
        client.close()
        jump.close()

    backup_ip = None
    for c in inventory:
        if c["service"] == "backup":
            backup_ip = c["ip"]

    if backup_ip and mariadb_ip:
        client, jump = validator._get_ssh_client(
            backup_ip, key_path
        )
        exit_code, output = validator._run_remote(
            client,
            f"nc -z -w 3 {mariadb_ip} 3306 && echo OK || echo FAIL"
        )
        if "OK" in output:
            print(f"  Backup -> MariaDB (3306): PASS")
            results["Connectivity backup->mariadb"] = "PASS"
        else:
            print(f"  Backup -> MariaDB (3306): FAIL")
            results["Connectivity backup->mariadb"] = "FAIL"
        client.close()
        jump.close()


def print_report(results):
    print("\n" + "=" * 50)
    print("  MIGRATION TEST REPORT")
    print("=" * 50)

    passed = 0
    failed = 0
    for test_name, status in results.items():
        icon = "PASS" if status == "PASS" else "FAIL"
        print(f"  {test_name:40s} {icon}")
        if status == "PASS":
            passed += 1
        else:
            failed += 1

    print("=" * 50)
    print(f"  Total: {passed + failed} tests | "
          f"PASSED: {passed} | FAILED: {failed}")

    if failed == 0:
        print("  Result: ALL TESTS PASSED")
    else:
        print(f"  Result: {failed} TEST(S) FAILED")
    print("=" * 50)


def main():
    print("=" * 50)
    print("  Post-Migration Test Suite")
    print("=" * 50)

    config = load_config()
    logger = setup_logger(config)

    username = input("OpenStack username: ")
    password = getpass.getpass("OpenStack password: ")

    net_manager = NetworkManager(config, None)
    conn = net_manager.connect(username, password)

    validator = Validator(config)

    key_path = config["paths"]["key_dir"] + "/migration-key"

    print("\nCollecting source data from LXC containers...")
    inventory, source_data = get_source_data()

    results = {}
    mariadb_ip = None

    for container in inventory:
        ip = container["ip"]
        service = container["service"]

        if service == "mariadb":
            mariadb_ip = ip
            test_mariadb_integrity(
                validator, ip, key_path, source_data, results
            )
        elif service == "apache":
            test_apache_service(
                validator, ip, key_path, results
            )
        elif service == "backup":
            test_backup_service(
                validator, ip, key_path, mariadb_ip, results
            )
        elif service == "nfs":
            test_nfs_service(
                validator, ip, key_path, results
            )
        elif service == "ftp":
            test_ftp_service(
                validator, ip, key_path, "vsftpd", results
            )

    test_connectivity(validator, inventory, key_path, results)

    print_report(results)

    all_passed = all(v == "PASS" for v in results.values())
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
