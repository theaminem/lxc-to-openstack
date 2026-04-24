import sys
import getpass
from src.config_loader import load_config
from src.logger import setup_logger
from src.scanner import scan_all
from src.rollback import Rollback
from src.network_manager import NetworkManager
from src.provisioner import Provisioner
from src.backup_manager import BackupManager
from src.transfer import Transfer
from src.restorer import Restorer
from src.validator import Validator


def ask_continue():
    while True:
        response = input("\nContinue to next phase? (y/n): ").strip().lower()
        if response == "y":
            return
        elif response == "n":
            print("Migration paused. Relaunch to continue.")
            sys.exit(0)
        else:
            print("Please type 'y' or 'n'")

def main():
    print("=" * 50)
    print("  LXC to OpenStack Migration Tool")
    print("=" * 50)
    print()

    config = load_config()
    logger = setup_logger(config)
    logger.info("Configuration loaded")

    username = input("OpenStack username: ")
    password = getpass.getpass("OpenStack password: ")
    jump_user = input("VM-cible SSH username: ")
    jump_password = getpass.getpass("VM-cible SSH password: ")
    logger.info(f"Authenticating as {username}")
    config["jump"] = {
        "username": jump_user,
        "password": jump_password
    }

    rollback = Rollback()
    conn = None

    try:
        logger.info("=" * 40)
        logger.info("PHASE 1: Scanning source infrastructure")
        logger.info("=" * 40)
        inventory = scan_all()
        logger.info(f"Found {len(inventory)} containers:")
        for c in inventory:
            logger.info(
                f"  {c['name']:12s} IP={c['ip']} "
                f"service={c['service']} ports={c['ports']}"
            )

        if not inventory:
            logger.error("No containers found. Aborting.")
            sys.exit(1)

        logger.info("PHASE 1 COMPLETE")
        ask_continue()

        logger.info("=" * 40)
        logger.info("PHASE 2: Setting up OpenStack network")
        logger.info("=" * 40)
        net_manager = NetworkManager(config, rollback)
        conn = net_manager.connect(username, password)
        network, subnet, ports = net_manager.setup_migration_network(
            inventory
        )
        logger.info("PHASE 2 COMPLETE")
        ask_continue()

        logger.info("=" * 40)
        logger.info("PHASE 3-4: Provisioning instances")
        logger.info("=" * 40)
        provisioner = Provisioner(config, rollback)
        provisioner.set_connection(conn)
        instances, private_key_path = provisioner.provision_all(
            inventory, ports
        )
        logger.info("PHASE 3-4 COMPLETE")
        ask_continue()

        logger.info("=" * 40)
        logger.info("PHASE 5: Backing up source data")
        logger.info("=" * 40)
        backup_mgr = BackupManager(config)
        backup_paths = backup_mgr.backup_all(inventory)
        logger.info("PHASE 5 COMPLETE")
        ask_continue()

        logger.info("=" * 40)
        logger.info("PHASE 6: Transferring data to instances")
        logger.info("=" * 40)
        transfer = Transfer(config)
        transfer.transfer_all(
            inventory, backup_paths, private_key_path, ports
        )
        logger.info("PHASE 6 COMPLETE")
        ask_continue()

        logger.info("=" * 40)
        logger.info("PHASE 7: Installing and restoring services")
        logger.info("=" * 40)
        restorer = Restorer(config)
        restorer.restore_all(
            inventory, backup_paths, private_key_path, ports
        )
        logger.info("PHASE 7 COMPLETE")
        ask_continue()

        logger.info("=" * 40)
        logger.info("PHASE 8: Validating migration")
        logger.info("=" * 40)
        validator = Validator(config)
        results = validator.validate_all(
            inventory, backup_paths, private_key_path, ports
        )

        all_passed = all(results.values())
        if all_passed:
            logger.info("MIGRATION SUCCESSFUL")
        else:
            logger.warning("MIGRATION COMPLETED WITH FAILURES")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        logger.info("Starting rollback...")
        try:
            rollback.execute(conn)
        except Exception:
            rollback.execute(None)
        sys.exit(1)


if __name__ == "__main__":
    main()
