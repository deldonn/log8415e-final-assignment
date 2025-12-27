"""
SSH client utilities using paramiko for remote command execution.
"""
import time
import socket
from pathlib import Path
from typing import Optional, Tuple
import paramiko
from .keypair import get_key_path


class SSHClient:
    """SSH client wrapper for remote command execution."""
    
    def __init__(self, host: str, username: str = "ubuntu", key_path: Optional[Path] = None):
        self.host = host
        self.username = username
        self.key_path = key_path or get_key_path()
        self.client: Optional[paramiko.SSHClient] = None
    
    def connect(self, retries: int = 10, delay: int = 15) -> bool:
        """
        Connect to the remote host with retries.
        Returns True if successful.
        """
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        for attempt in range(1, retries + 1):
            try:
                print(f"  [{attempt}/{retries}] Connecting to {self.host}...")
                self.client.connect(
                    hostname=self.host,
                    username=self.username,
                    key_filename=str(self.key_path),
                    timeout=10,
                    banner_timeout=30
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
            command: Command to execute
            sudo: Whether to run with sudo
            check: Whether to raise exception on non-zero exit
            
        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        if sudo:
            command = f"sudo bash -c '{command}'"
        
        stdin, stdout, stderr = self.client.exec_command(command, timeout=300)
        
        exit_code = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8', errors='replace')
        stderr_str = stderr.read().decode('utf-8', errors='replace')
        
        if check and exit_code != 0:
            print(f"  [ERROR] Command failed (exit {exit_code}): {command[:100]}")
            print(f"  STDERR: {stderr_str[:500]}")
            raise RuntimeError(f"Command failed with exit code {exit_code}")
        
        return exit_code, stdout_str, stderr_str
    
    def run_script(self, script: str, sudo: bool = True) -> Tuple[int, str, str]:
        """
        Execute a multi-line script on the remote host.
        """
        # Write script to temp file and execute
        script_escaped = script.replace("'", "'\\''")
        return self.run(f"echo '{script_escaped}' | bash", sudo=sudo)
    
    def upload_file(self, local_path: Path, remote_path: str):
        """Upload a file to the remote host."""
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")
        
        sftp = self.client.open_sftp()
        try:
            sftp.put(str(local_path), remote_path)
        finally:
            sftp.close()
    
    def wait_for_cloud_init(self, timeout: int = 600):
        """Wait for cloud-init to complete."""
        print(f"  [WAIT] Waiting for cloud-init to complete...")
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
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


def wait_for_ssh(host: str, timeout: int = 300) -> bool:
    """Wait for SSH to become available on a host."""
    print(f"  [WAIT] Waiting for SSH on {host}...")
    start = time.time()
    
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, 22))
            sock.close()
            
            if result == 0:
                print(f"  [OK] SSH is available on {host}")
                return True
        except socket.error:
            pass
        
        time.sleep(10)
    
    print(f"  [ERROR] Timeout waiting for SSH on {host}")
    return False


