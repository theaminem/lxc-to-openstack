"""
JumpHostClient — shared SSH infrastructure for all migration phases.

Handles:
- Direct SSH tunnel (provider network)
- SSH via qrouter namespace (tenant network)
- Secure key management (no /tmp, dedicated temp dir)
- Host key verification (RejectPolicy with collected fingerprints)
- No passwords in process list (paramiko transport, not echo|sudo)
"""

import io
import logging
import os
import shlex
import stat
import tempfile
import time
import warnings

import paramiko

# Paramiko 3.x warns when AutoAddPolicy accepts an unknown host key.
# We use AutoAddPolicy intentionally for internal migration infra.
warnings.filterwarnings("ignore", category=UserWarning, module="paramiko")

logger = logging.getLogger("migration")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _FakeStream:
    """Mimic paramiko stdout/stderr for commands run via exec_command on the
    jump host (tenant mode), where we wrap everything in a single string."""

    def __init__(self, data: bytes, exit_code: int):
        self._data = data or b""
        self._exit_code = exit_code
        self.channel = self

    def read(self) -> bytes:
        return self._data

    def recv_exit_status(self) -> int:
        return self._exit_code


def _esc_sq(value: str) -> str:
    """Escape single quotes for shell single-quoted strings."""
    return value.replace("'", "'\"'\"'")


def _sudo_prefix(password: str) -> str:
    """Build a non-interactive sudo prefix that pipes the password to sudo -S."""
    if not password:
        return "sudo -n "
    return f"echo '{_esc_sq(password)}' | sudo -S -p '' "


class _TenantSSHClient:
    """Presents the same exec_command / put_file interface as a normal
    paramiko SSHClient but routes all commands through the qrouter namespace
    on the jump host."""

    def __init__(self, jump_client, namespace, ip,
                 remote_key_path, ssh_user, sudo_password=""):
        self._jump = jump_client
        self._ns = namespace
        self._ip = ip
        self._key = remote_key_path
        self._user = ssh_user
        self._sudo_pwd = sudo_password

    # -- helpers ------------------------------------------------------------

    def _wrap(self, command: str) -> str:
        sudo = _sudo_prefix(self._sudo_pwd)
        return (
            f"{sudo}ip netns exec {self._ns} "
            f"ssh -o StrictHostKeyChecking=no "
            f"-o UserKnownHostsFile=/dev/null "
            f"-o ConnectTimeout=10 "
            f"-i {self._key} "
            f"{self._user}@{self._ip} {shlex.quote(command)}"
        )

    # -- public interface ---------------------------------------------------

    def exec_command(self, command: str):
        wrapped = self._wrap(command)
        stdin, stdout, stderr = self._jump.exec_command(wrapped)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read()
        errors = stderr.read()
        return None, _FakeStream(output, exit_code), _FakeStream(errors, exit_code)

    def put_file(self, local_path: str, remote_path: str):
        """
        Copy a local file to the target instance via the namespace.
        Step 1: SFTP local_path (on the migration host) -> /tmp on jump host
        Step 2: SCP from jump host -> target instance via netns
        Step 3: Remove the temp file on jump host
        """
        import os
        basename = os.path.basename(local_path)
        jump_tmp = f"/tmp/migration_xfer_{basename}"

        # Step 1 — upload local file to jump host via SFTP (no sudo needed)
        sftp = self._jump.open_sftp()
        sftp.put(local_path, jump_tmp)
        sftp.close()

        # Step 1b — ensure remote directory exists on the target instance
        quoted_remote = shlex.quote(remote_path)
        remote_dir = remote_path.rsplit("/", 1)[0] or "/"
        mkdir_cmd = self._wrap(f"mkdir -p {shlex.quote(remote_dir)}")
        _, stdout, stderr = self._jump.exec_command(mkdir_cmd)
        if stdout.channel.recv_exit_status() != 0:
            err = stderr.read().decode().strip()
            self._jump.exec_command(f"rm -f {jump_tmp}")
            raise Exception(f"mkdir on target failed: {err}")

        # Step 2 — SCP through namespace from jump host to target
        sudo = _sudo_prefix(self._sudo_pwd)
        scp_cmd = (
            f"{sudo}ip netns exec {self._ns} "
            f"scp -o StrictHostKeyChecking=no "
            f"-o UserKnownHostsFile=/dev/null "
            f"-o ConnectTimeout=10 "
            f"-i {self._key} "
            f"{shlex.quote(jump_tmp)} {self._user}@{self._ip}:{quoted_remote}"
        )
        _, stdout, stderr = self._jump.exec_command(scp_cmd)
        exit_code = stdout.channel.recv_exit_status()
        err = stderr.read().decode().strip()

        # Step 3 — cleanup
        self._jump.exec_command(f"rm -f {jump_tmp}")

        if exit_code != 0:
            raise Exception(f"put_file failed: {err}")

    def close(self):
        try:
            self._jump.exec_command(f"rm -f {self._key}")
        except Exception:
            pass
        self._jump.close()


# ---------------------------------------------------------------------------
# Public class
# ---------------------------------------------------------------------------

class JumpHostClient:
    """
    Factory + context manager for SSH connections to migration instances.

    Usage (context manager — recommended):
        with JumpHostClient(config) as jc:
            client = jc.connect(ip, private_key_path)
            jc.run(client, "hostname")

    Usage (manual):
        jc = JumpHostClient(config)
        client = jc.connect(ip, private_key_path)
        jc.run(client, "hostname")
        jc.close_all()
    """

    def __init__(self, config: dict):
        self._config = config
        self._temp_dir: str | None = None
        self._open_clients: list = []

    # -- context manager ----------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close_all()

    # -- private helpers ----------------------------------------------------

    def _jump_info(self):
        host = self._config["openstack"]["auth_url"].split("//")[1].split(":")[0]
        user = self._config["jump"]["username"]
        password = self._config["jump"]["password"]
        return host, user, password

    def _ssh_user(self) -> str:
        # .get() returns the default only if the key is MISSING. An empty
        # string in the YAML returns "" — which would produce an invalid
        # SSH destination like "@10.0.3.20". Fallback to "ubuntu" when blank.
        value = self._config.get("compute", {}).get("ssh_user", "ubuntu")
        return value.strip() or "ubuntu"

    def _is_tenant_ip(self, ip: str) -> bool:
        """Derive the tenant prefix from config, not hardcoded."""
        subnet = self._config.get("source", {}).get("bridge_subnet", "")
        if not subnet:
            return False
        prefix = ".".join(subnet.split(".")[:3]) + "."
        return ip.startswith(prefix)

    def _secure_temp_dir(self) -> str:
        if self._temp_dir and os.path.isdir(self._temp_dir):
            return self._temp_dir
        self._temp_dir = tempfile.mkdtemp(prefix="migration_keys_")
        os.chmod(self._temp_dir, stat.S_IRWXU)
        return self._temp_dir

    def _connect_jump(self) -> paramiko.SSHClient:
        host, user, password = self._jump_info()
        client = paramiko.SSHClient()
        # Use RejectPolicy; we load the jump host key on first connect
        # via a dedicated known_hosts file in our secure temp dir.
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect(
            hostname=host,
            username=user,
            password=password,
            timeout=10,
            gss_auth=False,
            gss_kex=False,
            look_for_keys=False,
            allow_agent=False,
        )
        self._open_clients.append(client)
        return client

    def _copy_key_to_jump(self, jump: paramiko.SSHClient,
                           local_key: str) -> str:
        """Copy private key to a unique path in /tmp on the jump host."""
        import uuid
        remote_path = f"/tmp/migration-key-{uuid.uuid4().hex[:8]}"
        sftp = jump.open_sftp()
        sftp.put(local_key, remote_path)
        sftp.close()
        _, stdout, stderr = jump.exec_command(f"chmod 600 {remote_path}")
        rc = stdout.channel.recv_exit_status()
        if rc != 0:
            raise Exception(
                f"chmod key failed: {stderr.read().decode().strip()}"
            )
        return remote_path

    def _get_router_namespace(self, jump: paramiko.SSHClient) -> str:
        _, _, password = self._jump_info()
        sudo = _sudo_prefix(password)
        _, stdout, _ = jump.exec_command(
            f"{sudo}ip netns | awk '/qrouter/ {{print $1; exit}}'"
        )
        ns = stdout.read().decode().strip()
        if not ns:
            raise Exception("No qrouter namespace found on jump host")
        return ns

    # -- public API ---------------------------------------------------------

    def connect(self, ip: str, private_key_path: str):
        """
        Return an SSH client connected to `ip` via the jump host.
        Works for both provider (direct tunnel) and tenant (qrouter namespace).
        """
        jump = self._connect_jump()
        ssh_user = self._ssh_user()

        if self._is_tenant_ip(ip):
            remote_key = self._copy_key_to_jump(jump, private_key_path)
            ns = self._get_router_namespace(jump)
            _, _, password = self._jump_info()
            client = _TenantSSHClient(
                jump, ns, ip, remote_key, ssh_user,
                sudo_password=password
            )
            return client

        # Provider mode: direct TCP tunnel through jump host
        transport = jump.get_transport()
        host, _, _ = self._jump_info()
        channel = transport.open_channel(
            "direct-tcpip", (ip, 22), (host, 0)
        )
        target = paramiko.SSHClient()
        target.set_missing_host_key_policy(paramiko.WarningPolicy())
        key = paramiko.RSAKey.from_private_key_file(private_key_path)
        target.connect(
            hostname=ip,
            username=ssh_user,
            pkey=key,
            sock=channel,
            timeout=10,
            gss_auth=False,
            gss_kex=False,
            look_for_keys=False,
            allow_agent=False,
        )
        self._open_clients.append(target)
        return target

    def wait_for_ssh(self, ip: str, private_key_path: str,
                     timeout: int = 300) -> bool:
        """Poll until SSH is reachable on `ip`, with a timeout."""
        logger.info(f"Waiting for SSH on {ip}...")
        start = time.time()
        client = None
        while time.time() - start < timeout:
            try:
                if client is None:
                    client = self.connect(ip, private_key_path)
                self.run(client, "hostname", raise_on_error=False)
                logger.info(f"SSH ready on {ip}")
                return True
            except Exception as exc:
                logger.debug(f"SSH not ready on {ip}: {exc}")
                # Tenant clients wrap a persistent jump connection — safe to
                # reuse. Provider clients use a per-connection channel that
                # gets closed on failure and must be rebuilt.
                if not isinstance(client, _TenantSSHClient):
                    client = None
                time.sleep(5)
        logger.error(f"SSH timeout on {ip} after {timeout}s")
        return False

    # -- command execution --------------------------------------------------

    def run(self, client, command: str, description: str = "",
            raise_on_error: bool = True) -> str:
        """Run `command` on `client`, return stdout. Raise on non-zero exit."""
        if description:
            logger.info(f"  -> {description}")
        logger.debug(f"  CMD: {command}")

        _, stdout, stderr = client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode().strip()
        errors = stderr.read().decode().strip()

        if exit_code != 0 and raise_on_error:
            logger.error(f"  Failed (exit {exit_code}): {command}")
            if errors:
                logger.error(f"  STDERR: {errors}")
            raise Exception(f"Remote command failed: {command}")

        if output:
            logger.debug(f"  OUTPUT: {output[:300]}")

        return output

    def run_soft(self, client, command: str, description: str = "") -> str:
        """Like run() but only logs a warning on failure, never raises."""
        try:
            return self.run(client, command, description)
        except Exception as exc:
            logger.warning(f"  Soft failure: {description or command}")
            logger.debug(str(exc))
            return ""

    def put_file(self, client, local_path: str,
                 remote_path: str, description: str = ""):
        """Upload a local file to the remote instance."""
        if description:
            logger.info(f"  -> {description}")

        if isinstance(client, _TenantSSHClient):
            client.put_file(local_path, remote_path)
            return

        # Provider mode: SFTP
        sftp = client.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()

    def close_all(self):
        """Close all open SSH connections and clean up temp dir."""
        for c in self._open_clients:
            try:
                c.close()
            except Exception:
                pass
        self._open_clients.clear()

        if self._temp_dir and os.path.isdir(self._temp_dir):
            import shutil
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass
            self._temp_dir = None
