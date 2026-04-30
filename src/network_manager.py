import openstack
import logging
from src.rollback import Rollback


logger = logging.getLogger("migration")


class NetworkManager:

    def __init__(self, config, rollback):
        self.config = config
        self.rollback = rollback
        self.conn = None
        self.network_mode = None

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
    def _try_tenant_network(self, source_cidr, source_gateway):
        base = source_cidr.split("/")[0].rsplit(".", 1)[0]
        try: 
            network = self.conn.network.find_network("migration-net")
            if network:
                logger.info(
                    "Network migration-net already exists, reusing"
                )
            else:
                network = self.conn.network.create_network(
                    name="migration-net"
                )
                self.rollback.register(
                    "network", network.id, "migration-net"
                )
                logger.info("Created tenant network: migration-net")

            subnet = self.conn.network.find_subnet("migration-subnet")
            if subnet:
                logger.info(
                    "Subnet migration-subnet already exists, reusing"
                )
            else:
                subnet = self.conn.network.create_subnet(
                    name="migration-subnet",
                    network_id=network.id,
                    cidr=source_cidr,
                    ip_version=4,
                    gateway_ip=source_gateway,
                    dns_nameservers=["8.8.8.8"],
                    allocation_pools=[{
                        "start": base + ".10",
                        "end": base + ".250"
                    }],
                    enable_dhcp=True
                )
                self.rollback.register(
                    "subnet", subnet.id, "migration-subnet"
                )
                logger.info(
                    f"Created subnet: migration-subnet ({source_cidr})"
                )

            ext_network_name = self.config["network"].get(
                "external_network", "provider"
            )
            ext_network = self.conn.network.find_network(
                ext_network_name
            )

            router = self.conn.network.find_router("migration-router")
            if router:
                logger.info(
                    "Router migration-router already exists, reusing"
                )
            else:
                if not ext_network:
                    raise Exception(
                        f"External network {ext_network_name} not found"
                    )
                router = self.conn.network.create_router(
                    name="migration-router",
                    external_gateway_info={
                        "network_id": ext_network.id
                    }
                )
                self.rollback.register(
                    "router", router.id, "migration-router"
                )
                self.conn.network.add_interface_to_router(
                    router.id, subnet_id=subnet.id
                )
                logger.info("Created router: migration-router")

            self.network_mode = "tenant"
            logger.info(
                "Tenant network mode: instances keep original IPs"
            )
            return network, subnet

        except Exception as e:
            logger.warning(
                f"Tenant network failed: {e}"
            )
            logger.info("Falling back to provider network...")
            return None, None

    def _use_provider_network(self):
        net_config = self.config["network"]
        provider_name = net_config.get(
            "external_network", "provider"
        )

        network = self.conn.network.find_network(provider_name)
        if not network:
            raise Exception(
                f"Provider network {provider_name} not found"
            )

        subnets = list(
            self.conn.network.subnets(network_id=network.id)
        )
        if not subnets:
            raise Exception("No subnet on provider network")
        subnet = subnets[0]

        self.network_mode = "provider"
        logger.info(
            f"Provider network mode: {provider_name} "
            f"(IPs will be remapped)"
        )
        return network, subnet

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
            logger.info(
                f"Security group {name} already exists, reusing"
            )
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

    def setup_security_groups(self, cidr):
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

        sg_icmp = self.find_or_create_security_group(
            "sg-icmp", "Allow ICMP ping"
        )
        self.add_sg_rule(sg_icmp.id, "ingress", "icmp", None, None)

        logger.info("All security groups configured")

        return {
            "ssh": sg_ssh,
            "http": sg_http,
            "mariadb": sg_mariadb,
            "backup": sg_backup,
            "nfs": sg_nfs,
            "ftp": sg_ftp,
            "icmp": sg_icmp
        }

    def get_security_groups_for_service(self, service,
                                         security_groups):
        sg_ssh = security_groups["ssh"]
        sg_icmp = security_groups["icmp"]

        if service == "mariadb":
            return [sg_ssh, sg_icmp, security_groups["mariadb"]]
        elif service == "apache":
            return [sg_ssh, sg_icmp, security_groups["http"]]
        elif service == "backup":
            return [sg_ssh, sg_icmp, security_groups["backup"]]
        elif service == "nfs":
            return [sg_ssh, sg_icmp, security_groups["nfs"]]
        elif service == "ftp":
            return [sg_ssh, sg_icmp, security_groups["ftp"]]
        else:
            return [sg_ssh, sg_icmp]

    def setup_migration_network(self, inventory):
        source_cidr = self.config["source"]["bridge_subnet"]
        parts = source_cidr.split("/")
        base = parts[0].rsplit(".", 1)[0]
        source_gateway = base + ".1"

        security_groups = self.setup_security_groups(source_cidr)

        network, subnet = self._try_tenant_network(
            source_cidr, source_gateway
        )

        if network is None:
            network, subnet = self._use_provider_network()

        known_services = [
            "mariadb", "apache", "backup", "nfs", "ftp"
        ]

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

            if self.network_mode == "tenant":
                ip = container["ip"]
                port = self.find_or_create_port(
                    network.id, subnet.id, ip, port_name, sgs
                )
            else:
                port = self.find_or_create_port(
                    network.id, subnet.id, None, port_name, sgs
                )
            ports[name] = port

        logger.info(
            f"Network setup complete: {len(ports)} ports created "
            f"(mode: {self.network_mode})"
        )
        return network, subnet, ports
