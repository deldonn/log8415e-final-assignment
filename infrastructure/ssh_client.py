"""
SSH Client - LOG8415E Final Assignment

Provides SSH connectivity to EC2 instances using paramiko.
Used for remote command execution during setup and configuration.
"""
import time
import socket
from pathlib import Path
from typing import Optional, Tuple
import paramiko
from .keypair import get_key_path


# =============================================================================
# Key Loading
# =============================================================================

def load_private_key(key_path: Path):
    """
    Load a private key from file, trying multiple formats.
    
    Attempts to load the key as RSA, Ed25519, ECDSA, or DSS.
    AWS typically uses RSA keys.
    
    Args:
        key_path: Path to the private key file
    
    Returns:
        Paramiko key object
    
    Raises:
        ValueError: If key cannot be loaded in any format
    """
    key_path_str = str(key_path)
    
    # Try each key type
    for key_class in [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.DSSKey]:
        try:
            return key_class.from_private_key_file(key_path_str)
        except Exception:
            continue
    
    raise ValueError(f"Cannot load private key from {key_path}")


# =============================================================================
# SSH Client Class
# =============================================================================

class SSHClient:
    """
    SSH client wrapper for remote command execution.
    
    Supports context manager protocol for automatic connection handling.
    
    Example:
        with SSHClient("1.2.3.4") as ssh:
            ssh.run("apt-get update", sudo=True)
    """
    
    def __init__(self, host: str, username: str = "ubuntu", key_path: Optional[Path] = None):
        """
        Initialize SSH client.
        
        Args:
            host: IP address or hostname to connect to
            username: SSH username (default: ubuntu for AWS)
            key_path: Path to private key (auto-detected if not provided)
        """
        self.host = host
        self.username = username
        self.key_path = key_path or get_key_path()
        self.client: Optional[paramiko.SSHClient] = None
        self._pkey = None
    
    def connect(self, retries: int = 10, delay: int = 15) -> bool:
        """
        Connect to the remote host with retries.
        
        Handles common connection failures (instance not ready, SSH not started).
        
        Args:
            retries: Maximum connection attempts
            delay: Seconds between retries
        
        Returns:
            True if connected successfully
        """
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Load key once
        if self._pkey is None:
            try:
                self._pkey = load_private_key(self.key_path)
            except Exception as e:
                print(f"  [ERROR] Cannot load SSH key: {e}")
                return False
        
        for attempt in range(1, retries + 1):
            try:
                print(f"  [{attempt}/{retries}] Connecting to {self.host}...")
                self.client.connect(
                    hostname=self.host,
                    username=self.username,
                    pkey=self._pkey,
                    timeout=10,
                    banner_timeout=30,
                    allow_agent=False,
                    look_for_keys=False
                )
                print(f"  [OK] Connected to {self.host}")
                return True
            except (paramiko.ssh_exception.NoValidConnectionsError,
                    paramiko.ssh_exception.SSHException,
                    socket.timeout,
                    socket.error,
                    EOFError) as e:
                if attempt < retries:
                    print(f"  [WAIT] Connection failed ({e}), retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    print(f"  [ERROR] Failed to connect after {retries} attempts")
                    return False
        return False
    
    def disconnect(self):
        """Close the SSH connection."""
        if self.client:
            self.client.close()
            self.client = None
    
    def run(self, command: str, sudo: bool = False, check: bool = True) -> Tuple[int, str, str]:
        """
        Execute a command on the remote host.
        
        Args:
            command: Shell command to execute
            sudo: Run with sudo privileges
            check: Raise exception on non-zero exit code
        
        Returns:
            Tuple of (exit_code, stdout, stderr)
        
        Raises:
            RuntimeError: If check=True and command fails
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        if sudo:
            # Escape single quotes for bash -c
            escaped_cmd = command.replace("'", "'\"'\"'")
            command = f"sudo bash -c '{escaped_cmd}'"
        
        stdin, stdout, stderr = self.client.exec_command(command, timeout=300)
        
        exit_code = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8', errors='replace')
        stderr_str = stderr.read().decode('utf-8', errors='replace')
        
        if check and exit_code != 0:
            print(f"  [ERROR] Command failed (exit {exit_code}): {command[:100]}")
            print(f"  STDERR: {stderr_str[:500]}")
            raise RuntimeError(f"Command failed with exit code {exit_code}")
        
        return exit_code, stdout_str, stderr_str
    
    def wait_for_cloud_init(self, timeout: int = 600) -> bool:
        """
        Wait for cloud-init to complete on the instance.
        
        Cloud-init runs on first boot to configure the instance.
        We wait for it to finish before running our setup commands.
        
        Args:
            timeout: Maximum wait time in seconds
        
        Returns:
            True if cloud-init completed (or errored)
        """
        print(f"  [WAIT] Waiting for cloud-init...")
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                exit_code, stdout, _ = self.run("cloud-init status", check=False)
                if "done" in stdout:
                    print("  [OK] Cloud-init completed")
                    return True
                elif "error" in stdout:
                    print("  [WARN] Cloud-init finished with errors")
                    return True
            except Exception:
                pass
            time.sleep(10)
        
        print("  [WARN] Timeout waiting for cloud-init")
        return False
    
    def __enter__(self):
        """Context manager entry - connect to host."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - disconnect from host."""
        self.disconnect()


# =============================================================================
# Utility Functions
# =============================================================================

def wait_for_ssh(host: str, timeout: int = 300) -> bool:
    """
    Wait for SSH port to become available on a host.
    
    Polls port 22 until it accepts connections.
    Used before attempting SSH connection to new instances.
    
    Args:
        host: IP address or hostname
        timeout: Maximum wait time in seconds
    
    Returns:
        True if SSH is available
    """
    print(f"  [WAIT] Waiting for SSH on {host}...")
    start = time.time()
    
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, 22))
            sock.close()
            
            if result == 0:
                print(f"  [OK] SSH available on {host}")
                return True
        except socket.error:
            pass
        
        time.sleep(10)
    
    print(f"  [ERROR] Timeout waiting for SSH on {host}")
    return False
