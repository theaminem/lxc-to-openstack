"""
LXC → OpenStack Migration Tool  (v2)

Usage:
  python migrate.py                          # interactive, asks between phases
  python migrate.py --yes                    # no ask_continue() prompts
  python migrate.py --resume-from phase5     # skip phases 1-4, load saved state
  python migrate.py --dry-run                # phase 1 only (scan + plan), nothing created
"""

import argparse
import getpass
import json
import os
import sys

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

STATE_FILE = "migration_state.json"

PHASES = [
    "phase1",   # scan
    "phase2",   # network
    "phase3",   # provision
    "phase4",   # backup
    "phase5",   # transfer
    "phase6",   # restore
    "phase7",   # validate
]


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def ask_continue(auto_yes: bool = False):
    if auto_yes:
        return
    while True:
        response = input("\nContinue to next phase? (y/n): ").strip().lower()
        if response == "y":
            return
        elif response == "n":
            print("Migration paused. Relaunch to continue.")
            sys.exit(0)
        else:
            print("Please type 'y' or 'n'")


def parse_args():
    parser = argparse.ArgumentParser(
        description="LXC to OpenStack migration tool"
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompts between phases"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and display plan only — nothing is created"
    )
    parser.add_argument(
        "--resume-from",
        choices=PHASES,
        metavar="PHASE",
        help=(
            f"Resume from a specific phase, loading saved state. "
            f"Choices: {', '.join(PHASES)}"
        )
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    auto_yes = args.yes or args.dry_run

    print("=" * 50)
    print("  LXC to OpenStack Migration Tool  (v2)")
    print("=" * 50)
    print()

    config = load_config()
    logger = setup_logger(config)
    logger.info("Configuration loaded")

    # ------------------------------------------------------------------
    # Credentials (asked once, stored only in memory)
    # ------------------------------------------------------------------
    state = {}
    resume_phase = PHASES.index(args.resume_from) if args.resume_from else 0

    if resume_phase > 0:
        state = load_state()
        if not state:
            logger.error(
                f"No saved state found ({STATE_FILE}). "
                f"Cannot resume from {args.resume_from}."
            )
            sys.exit(1)
        logger.info(f"Resuming from {args.resume_from}")

    username = input("OpenStack username: ")
    password = getpass.getpass("OpenStack password: ")
    jump_user = input("Jump host SSH username: ")
    jump_password = getpass.getpass("Jump host SSH password: ")

    config["jump"] = {"username": jump_user, "password": jump_password}

    # MariaDB app password (asked only if restore/validate phases will run)
    db_password = ""
    if resume_phase <= 5:
        db_password = getpass.getpass(
            "MariaDB app user password (new instances): "
        )

    rollback = Rollback()
    conn = None

    try:
        # ==================================================================
        # PHASE 1 — Scan
        # ==================================================================
        if resume_phase <= 0:
            logger.info("=" * 40)
            logger.info("PHASE 1: Scanning source LXC infrastructure")
            logger.info("=" * 40)

            inventory = scan_all()

            if not inventory:
                logger.error("No active containers found. Aborting.")
                sys.exit(1)

            logger.info(f"Found {len(inventory)} containers:")
            for c in inventory:
                logger.info(
                    f"  {c['name']:15s}  IP={c['ip']}  "
                    f"OS={c.get('os', {}).get('codename', '?'):8s}  "
                    f"service={c['service']}  ports={c['ports']}"
                )

            if args.dry_run:
                logger.info("")
                logger.info("DRY RUN — plan summary:")
                for c in inventory:
                    os_info = c.get("os", {})
                    logger.info(
                        f"  {c['name']:15s}  → instance  "
                        f"image={os_info.get('codename','?')}  "
                        f"RAM~{c.get('ram_mb', {}).get('used', '?')}MB  "
                        f"Disk~{c.get('disk_mb', '?')}MB"
                    )
                logger.info("Dry run complete. Nothing was created.")
                return

            state["inventory"] = inventory
            save_state(state)
            logger.info("PHASE 1 COMPLETE")
            ask_continue(auto_yes)
        else:
            inventory = state["inventory"]
            logger.info(
                f"Phase 1 skipped (loaded {len(inventory)} containers from state)"
            )

        # ==================================================================
        # PHASE 2 — Network
        # ==================================================================
        if resume_phase <= 1:
            logger.info("=" * 40)
            logger.info("PHASE 2: Setting up OpenStack network")
            logger.info("=" * 40)

            net_manager = NetworkManager(config, rollback)
            conn = net_manager.connect(username, password)
            network, subnet, ports = net_manager.setup_migration_network(
                inventory
            )

            # Serialize ports for state (store IP only)
            state["ports_ips"] = {
                name: port.fixed_ips[0]["ip_address"]
                for name, port in ports.items()
            }
            state["network_mode"] = net_manager.network_mode
            save_state(state)
            logger.info("PHASE 2 COMPLETE")
            ask_continue(auto_yes)
        else:
            if conn is None:
                net_manager = NetworkManager(config, rollback)
                conn = net_manager.connect(username, password)
            logger.info("Phase 2 skipped (loaded network state)")
            # Reconstruct ports objects
            ports = {
                name: conn.network.find_port(f"port-{name}")
                for name in state.get("ports_ips", {})
            }

        # ==================================================================
        # PHASE 3 — Provision
        # ==================================================================
        if resume_phase <= 2:
            logger.info("=" * 40)
            logger.info("PHASE 3: Provisioning instances")
            logger.info("=" * 40)

            provisioner = Provisioner(config, rollback)
            provisioner.set_connection(conn)
            instances, private_key_path = provisioner.provision_all(
                inventory, ports
            )

            state["private_key_path"] = private_key_path
            save_state(state)
            logger.info("PHASE 3 COMPLETE")
            ask_continue(auto_yes)
        else:
            private_key_path = state.get("private_key_path", "")
            logger.info("Phase 3 skipped (loaded key path from state)")

        # ==================================================================
        # PHASE 4 — Backup
        # ==================================================================
        if resume_phase <= 3:
            logger.info("=" * 40)
            logger.info("PHASE 4: Backing up source data")
            logger.info("=" * 40)

            backup_mgr = BackupManager(config)
            backup_paths = backup_mgr.backup_all(inventory)

            state["backup_paths"] = backup_paths
            save_state(state)
            logger.info("PHASE 4 COMPLETE")
            ask_continue(auto_yes)
        else:
            backup_paths = state.get("backup_paths", {})
            logger.info("Phase 4 skipped (loaded backup paths from state)")

        # ==================================================================
        # PHASE 5 — Transfer
        # ==================================================================
        if resume_phase <= 4:
            logger.info("=" * 40)
            logger.info("PHASE 5: Transferring data to instances")
            logger.info("=" * 40)

            transfer = Transfer(config)
            transfer.transfer_all(
                inventory, backup_paths, private_key_path, ports
            )

            save_state(state)
            logger.info("PHASE 5 COMPLETE")
            ask_continue(auto_yes)

        # ==================================================================
        # PHASE 6 — Restore
        # ==================================================================
        if resume_phase <= 5:
            logger.info("=" * 40)
            logger.info("PHASE 6: Installing and restoring services")
            logger.info("=" * 40)

            network_mode = state.get("network_mode", "provider")
            restorer = Restorer(config)
            restorer.restore_all(
                inventory, backup_paths, private_key_path, ports,
                network_mode=network_mode,
                db_password=db_password
            )

            if network_mode == "tenant":
                _assign_apache_fip(conn, config, inventory, ports, logger)

            logger.info("PHASE 6 COMPLETE")
            ask_continue(auto_yes)

        # ==================================================================
        # PHASE 7 — Validate
        # ==================================================================
        logger.info("=" * 40)
        logger.info("PHASE 7: Validating migration")
        logger.info("=" * 40)

        # Resolve db_user from inventory
        db_user = ""
        for c in inventory:
            users = c.get("app_users", [])
            if users:
                db_user = users[0]["user"]
                break

        validator = Validator(config)
        results = validator.validate_all(
            inventory, backup_paths, private_key_path, ports,
            db_user=db_user, db_password=db_password
        )

        all_passed = all(results.values())
        if all_passed:
            logger.info("MIGRATION SUCCESSFUL ✓")
        else:
            logger.warning("MIGRATION COMPLETED WITH FAILURES")

        # Summary
        logger.info("=" * 40)
        logger.info("INSTANCE SUMMARY")
        logger.info("=" * 40)
        for container in inventory:
            name = container["name"]
            if name in ports:
                ip = ports[name].fixed_ips[0]["ip_address"]
                logger.info(f"  {name:15s} : {ip}")

        network_mode = state.get("network_mode", "provider")
        if network_mode == "tenant" and conn:
            ext_net = conn.network.find_network(
                config["network"].get("external_network", "provider")
            )
            if ext_net:
                fips = list(conn.network.ips(
                    floating_network_id=ext_net.id,
                    project_id=conn.current_project_id
                ))
                if fips:
                    logger.info("")
                    logger.info("EXTERNAL ACCESS:")
                    for fip in fips:
                        logger.info(f"  http://{fip.floating_ip_address}")
        else:
            for container in inventory:
                if container["service"] == "apache":
                    name = container["name"]
                    if name in ports:
                        ip = ports[name].fixed_ips[0]["ip_address"]
                        logger.info(f"\nAPPLICATION: http://{ip}")
                    break

        logger.info("=" * 40)

        # Clean up state file on success
        if all_passed and os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            logger.info(f"Removed {STATE_FILE}")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        logger.info("Starting rollback...")
        try:
            rollback.execute(conn)
        except Exception:
            rollback.execute(None)
        sys.exit(1)


def _assign_apache_fip(conn, config, inventory, ports, logger):
    """Assign a floating IP to the Apache instance (tenant mode only)."""
    ext_net = conn.network.find_network(
        config["network"].get("external_network", "provider")
    )
    for container in inventory:
        if container["service"] == "apache":
            name = container["name"]
            if name not in ports:
                break
            port = ports[name]
            existing = list(conn.network.ips(port_id=port.id))
            if existing:
                fip = existing[0]
                logger.info(
                    f"Floating IP already assigned: {fip.floating_ip_address}"
                )
            else:
                fip = conn.network.create_ip(
                    floating_network_id=ext_net.id,
                    port_id=port.id
                )
                logger.info(
                    f"Floating IP assigned to Apache: {fip.floating_ip_address}"
                )
            logger.info(f"Access: http://{fip.floating_ip_address}")
            break


if __name__ == "__main__":
    main()
