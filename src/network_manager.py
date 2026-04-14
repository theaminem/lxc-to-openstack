import openstack
import logging
from src.rollback import Rollback


logger = logging.getLogger("migration")


class NetworkManager:

    def __init__(self, config, rollback):
        self.config = config
        self.rollback = rollback
        self.conn = None
        self.use_provider = config["network"].get("use_provider", False)

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
            name=name,
            network_id=network_id,
            cidr=net_config["cidr"],
            ip_version=4,
            gateway_ip=net_config["gateway"],
            allocation_pools=[{
                "start": net_config["pool_start"],
                "end": net_config["pool_end"]
            }]
        )
        self.rollback.register("subnet", subnet.id, name)
        logger.info(f"Created subnet: {name} ({net_config['cidr']})")
        return subnet

    def find_or_create_router(self, subnet_id):
        net_config = self.config["network"]
        router_name = net_config["router_name"]
        ext_network_name = net_config["external_network"]

        try:
            existing = self.conn.network.find_router(router_name)
        except Exception:
            existing = None

        if existing:
            logger.info(
                f"Router {router_name} already exists, reusing"
            )
            return existing

        ext_network = self.conn.network.find_network(ext_network_name)
        if not ext_network:
            raise Exception(
                f"External network {ext_network_name} not found"
            )

        router = self.conn.network.create_router(
            name=router_name,
            external_gateway_info={"network_id": ext_network.id}
        )
        self.rollback.register("router", router.id, router_name)
        logger.info(f"Created router: {router_name}")

        self.conn.network.add_interface_to_router(
            router.id,
            subnet_id=subnet_id
        )
        logger.info(
            f"Attached subnet to router {router_name}"
        )

        return router

    def find_or_create_port(self, network_id, subnet_id, ip,
                            port_name, security_groups):
        existing_ports = list(
            self.conn.network.ports(network_id=network_id)
        )
        for port in existing_ports:
            if port.name == port_name:
                logger.info(
                    f"Port {port_name} already exists, reusing"
                )
                return port

        sg_ids = [sg.id for sg in security_groups]
        port_params = {
            "name": port_name,
            "network_id": network_id,
            "security_group_ids": sg_ids
        }
        if ip:
            port_params["fixed_ips"] = [{
                "subnet_id": subnet_id,
                "ip_address": ip
            }]

        port = self.conn.network.create_port(**port_params)
        self.rollback.register("port", port.id, port_name)
        ip_assigned = port.fixed_ips[0]["ip_address"]
        logger.info(f"Created port: {port_name} ({ip_assigned})")
        return port

    def find_or_create_security_group(self, name, description):
        existing = self.conn.network.find_security_group(name)
        if existing:
            logger.info(f"Security group {name} already exists, reusing")
            return existing

        sg = self.conn.network.create_security_group(
            name=name,
            description=description
        )
        self.rollback.register("security_group", sg.id, name)
        logger.info(f"Created security group: {name}")
        return sg

    def add_sg_rule(self, sg_id, direction, protocol,
                    port_min, port_max, remote_ip=None):
        existing_rules = list(
            self.conn.network.security_group_rules(
                security_group_id=sg_id
            )
        )
        for rule in existing_rules:
            if (rule.direction == direction
                    and rule.protocol == protocol
                    and rule.port_range_min == port_min
                    and rule.port_range_max == port_max):
                return

        params = {
            "security_group_id": sg_id,
            "direction": direction,
            "protocol": protocol,
            "port_range_min": port_min,
            "port_range_max": port_max,
            "ethertype": "IPv4"
        }
        if remote_ip:
            params["remote_ip_prefix"] = remote_ip

        self.conn.network.create_security_group_rule(**params)
        logger.info(
            f"  Rule: {direction} {protocol} "
            f"{port_min}-{port_max} from {remote_ip or 'any'}"
        )

    def setup_security_groups(self):
        cidr = self.config["network"]["cidr"]

        sg_ssh = self.find_or_create_security_group(
            "sg-ssh", "Allow SSH access"
        )
        self.add_sg_rule(sg_ssh.id, "ingress", "tcp", 22, 22)

        sg_http = self.find_or_create_security_group(
            "sg-http", "Allow HTTP access"
        )
        self.add_sg_rule(sg_http.id, "ingress", "tcp", 80, 80)

        sg_mariadb = self.find_or_create_security_group(
            "sg-mariadb", "Allow MariaDB from internal network"
        )
        self.add_sg_rule(
            sg_mariadb.id, "ingress", "tcp", 3306, 3306, cidr
        )

        sg_backup = self.find_or_create_security_group(
            "sg-backup", "Allow backup to reach MariaDB"
        )
        self.add_sg_rule(
            sg_backup.id, "egress", "tcp", 3306, 3306, cidr
        )

        sg_nfs = self.find_or_create_security_group(
            "sg-nfs", "Allow NFS access from internal network"
        )
        self.add_sg_rule(
            sg_nfs.id, "ingress", "tcp", 2049, 2049, cidr
        )
        self.add_sg_rule(
            sg_nfs.id, "ingress", "tcp", 111, 111, cidr
        )

        sg_ftp = self.find_or_create_security_group(
            "sg-ftp", "Allow FTP access"
        )
        self.add_sg_rule(sg_ftp.id, "ingress", "tcp", 21, 21)

        logger.info("All security groups configured")

        return {
            "ssh": sg_ssh,
            "http": sg_http,
            "mariadb": sg_mariadb,
            "backup": sg_backup,
            "nfs": sg_nfs,
            "ftp": sg_ftp
        }

    def get_security_groups_for_service(self, service,
                                         security_groups):
        sg_ssh = security_groups["ssh"]

        if service == "mariadb":
            return [sg_ssh, security_groups["mariadb"]]
        elif service == "apache":
            return [sg_ssh, security_groups["http"]]
        elif service == "backup":
            return [sg_ssh, security_groups["backup"]]
        elif service == "nfs":
            return [sg_ssh, security_groups["nfs"]]
        elif service == "ftp":
            return [sg_ssh, security_groups["ftp"]]
        else:
            return [sg_ssh]

    def setup_migration_network(self, inventory):
        security_groups = self.setup_security_groups()

        known_services = [
            "mariadb", "apache", "backup", "nfs", "ftp"
        ]

        net_config = self.config["network"]

        if self.use_provider:
            network = self.conn.network.find_network(
                net_config["name"]
            )
            if not network:
                raise Exception(
                    f"Provider network {net_config['name']} not found"
                )
            subnets = list(
                self.conn.network.subnets(network_id=network.id)
            )
            if not subnets:
                raise Exception("No subnet on provider network")
            subnet = subnets[0]
            logger.info(
                f"Using provider network: {net_config['name']}"
            )
        else:
            network = self.find_or_create_network()
            subnet = self.find_or_create_subnet(network.id)
            router = self.find_or_create_router(subnet.id)
            logger.info("Tenant network created with router")

        ports = {}
        for container in inventory:
            name = container["name"]
            service = container["service"]
            port_name = f"port-{name}"

            if service not in known_services:
                logger.warning(
                    f"Unknown service {service} on {name}, "
                    f"skipping"
                )
                continue

            sgs = self.get_security_groups_for_service(
                service, security_groups
            )

            if self.use_provider:
                port = self.find_or_create_port(
                    network.id, subnet.id, None, port_name, sgs
                )
            else:
                ip = container["ip"]
                port = self.find_or_create_port(
                    network.id, subnet.id, ip, port_name, sgs
                )
            ports[name] = port

        logger.info(
            f"Network setup complete: {len(ports)} ports created"
        )
        return network, subnet, ports
