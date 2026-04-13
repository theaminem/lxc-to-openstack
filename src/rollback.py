import logging


logger = logging.getLogger("migration")


class Rollback:

    def __init__(self):
        self.created_resources = []

    def register(self, resource_type, resource_id, resource_name):
        self.created_resources.append({
            "type": resource_type,
            "id": resource_id,
            "name": resource_name
        })
        logger.debug(
            f"Registered for rollback: {resource_type} {resource_name}"
        )

    def execute(self, conn):
        if not self.created_resources:
            logger.info("Nothing to rollback")
            return

        if not conn:
            logger.error(
                "No OpenStack connection, cannot rollback"
            )
            return

        logger.info("Starting rollback...")
        reversed_resources = list(reversed(self.created_resources))

        for resource in reversed_resources:
            r_type = resource["type"]
            r_id = resource["id"]
            r_name = resource["name"]

            try:
                if r_type == "server":
                    conn.compute.delete_server(r_id)
                    logger.info(f"Deleted server: {r_name}")

                elif r_type == "volume":
                    volume = conn.block_storage.get_volume(r_id)
                    if volume.status == "in-use":
                        for att in volume.attachments:
                            conn.compute.delete_volume_attachment(
                                att["server_id"], r_id
                            )
                        logger.info(
                            f"Detached volume: {r_name}"
                        )
                        conn.block_storage.wait_for_status(
                            volume, status="available", wait=60
                        )
                    conn.block_storage.delete_volume(r_id)
                    logger.info(f"Deleted volume: {r_name}")

                elif r_type == "router":
                    ports = list(
                        conn.network.ports(device_id=r_id)
                    )
                    for port in ports:
                        if port.device_owner == "network:router_interface":
                            conn.network.remove_interface_from_router(
                                r_id, subnet_id=port.fixed_ips[0]["subnet_id"]
                            )
                    conn.network.delete_router(r_id)
                    logger.info(f"Deleted router: {r_name}")

                elif r_type == "port":
                    conn.network.delete_port(r_id)
                    logger.info(f"Deleted port: {r_name}")

                elif r_type == "security_group":
                    conn.network.delete_security_group(r_id)
                    logger.info(
                        f"Deleted security group: {r_name}"
                    )

                elif r_type == "subnet":
                    conn.network.delete_subnet(r_id)
                    logger.info(f"Deleted subnet: {r_name}")

                elif r_type == "network":
                    conn.network.delete_network(r_id)
                    logger.info(f"Deleted network: {r_name}")

                elif r_type == "keypair":
                    conn.compute.delete_keypair(r_id)
                    logger.info(f"Deleted keypair: {r_name}")

                elif r_type == "flavor":
                    conn.compute.delete_flavor(r_id)
                    logger.info(f"Deleted flavor: {r_name}")

            except Exception as e:
                logger.error(
                    f"Failed to delete {r_type} {r_name}: {e}"
                )

        self.created_resources.clear()
        logger.info("Rollback complete")
