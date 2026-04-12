import openstack
import logging
from src.rollback import Rollback


logger = logging.getLogger("migration")


class NetworkManager:

    def __init__(self, config, rollback):
        self.config = config
        self.rollback = rollback
        self.conn = None

    def connect(self, username, password):
        os_config = self.config["openstack"]
        self.conn = openstack.connect(
            auth_url=os_config["auth_url"],
            project_name=os_config["project_name"],
            username=username,
            password=password,
            user_domain_name=os_config["user_domain_name"],
            project_domain_name=os_config["project_domain_name"],
            region_name=os_config["region"]
        )
        logger.info("Connected to OpenStack")
        return self.conn

    def find_or_create_network(self):
        net_config = self.config["network"]
        name = net_config["name"]

        existing = self.conn.network.find_network(name)
        if existing:
            logger.info(f"Network {name} already exists, reusing")
            return existing

        network = self.conn.network.create_network(name=name)
        self.rollback.register("network", network.id, name)
        logger.info(f"Created network: {name}")
        return network

    def find_or_create_subnet(self, network_id):
        net_config = self.config["network"]
        name = net_config["subnet_name"]

        existing = self.conn.network.find_subnet(name)
        if existing:
            logger.info(f"Subnet {name} already exists, reusing")
            return existing

        subnet = self.conn.network.create_subnet(
