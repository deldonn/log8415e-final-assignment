"""
Key Pair Management - LOG8415E Final Assignment

Manages EC2 SSH key pairs for instance access.
Creates keys in ~/.ssh/ directory and handles Windows file locking issues.
"""
import os
import stat
import subprocess
import sys
import time
from pathlib import Path
from botocore.exceptions import ClientError
from .aws_client import get_ec2_client, get_config


# =============================================================================
# Path Utilities
# =============================================================================

def get_key_path() -> Path:
    """
    Get path to the private key file.
    
    Keys are stored in user's ~/.ssh directory.
    
    Returns:
        Path to .pem file
    """
    config = get_config()
    key_name = config["aws"]["key_pair_name"]
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(exist_ok=True)
    return ssh_dir / f"{key_name}.pem"


# =============================================================================
# Windows File Deletion Helpers
# =============================================================================

def safe_unlink(path: Path, retries: int = 5, delay_s: float = 0.2) -> bool:
    """
    Delete a file robustly on Windows.
    
    Handles common issues:
    - Read-only flag preventing deletion
    - File locked by antivirus or IDE
    
    Args:
        path: Path to file to delete
        retries: Number of retry attempts
        delay_s: Delay between retries
    
    Returns:
        True if file was deleted
    """
    # Remove read-only flag
    try:
        os.chmod(path, stat.S_IWRITE)
    except Exception:
        pass

    for _ in range(retries):
        try:
            path.unlink()
            return True
        except PermissionError:
            time.sleep(delay_s)
        except FileNotFoundError:
            return True
        except Exception:
            break

    return False


def force_delete_windows(path: Path) -> bool:
    """
    Last-resort file deletion using Windows commands.
    
    Uses attrib and del commands to force delete.
    
    Args:
        path: Path to file to delete
    
    Returns:
        True if file was deleted
    """
    try:
        subprocess.run(["attrib", "-R", str(path)], check=False, capture_output=True)
        subprocess.run(["cmd", "/c", "del", "/f", "/q", str(path)], check=True, capture_output=True)
        return not path.exists()
    except Exception:
        return False


# =============================================================================
# Key Pair Operations
# =============================================================================

def create_key_pair() -> str:
    """
    Create EC2 key pair if it doesn't exist.
    
    Creates RSA key pair in AWS and saves private key locally.
    Sets proper permissions (chmod 400) on Unix systems.
    
    Returns:
        Key pair name
    """
    config = get_config()
    ec2 = get_ec2_client()
    key_name = config["aws"]["key_pair_name"]
    key_path = get_key_path()

    # Check if key pair exists in AWS
    try:
        ec2.describe_key_pairs(KeyNames=[key_name])
        print(f"[OK] Key pair '{key_name}' exists")
        
        if not key_path.exists():
            print(f"[WARN] Private key not found at {key_path}")
            print("       Delete key pair and recreate if needed.")
        return key_name
        
    except ClientError as e:
        if "InvalidKeyPair.NotFound" not in str(e):
            raise

    # Create new key pair
    print(f"[+] Creating key pair '{key_name}'...")
    response = ec2.create_key_pair(KeyName=key_name, KeyType="rsa", KeyFormat="pem")

    # Save private key with proper line endings
    private_key = response["KeyMaterial"]
    private_key = private_key.replace('\r\n', '\n').replace('\r', '\n')
    if not private_key.endswith('\n'):
        private_key += '\n'
    
    with open(key_path, "w", newline='\n') as f:
        f.write(private_key)
    
    # Set permissions (Unix only)
    try:
        os.chmod(key_path, 0o400)
    except Exception:
        pass

    print(f"[OK] Key pair created: {key_path}")
    return key_name


def delete_key_pair():
    """
    Delete the EC2 key pair from AWS and local file.
    
    Uses robust deletion for Windows compatibility
    (handles file locks from IDEs/antivirus).
    """
    config = get_config()
    ec2 = get_ec2_client()
    key_name = config["aws"]["key_pair_name"]
    key_path = get_key_path()

    # Delete from AWS
    try:
        ec2.delete_key_pair(KeyName=key_name)
        print(f"[OK] Key pair '{key_name}' deleted from AWS")
    except ClientError as e:
        if "InvalidKeyPair.NotFound" not in str(e):
            raise
        print(f"[OK] Key pair '{key_name}' not found (already deleted)")

    # Remove local key file with robust deletion
    if key_path.exists():
        deleted = safe_unlink(key_path)

        # Last resort on Windows
        if not deleted and sys.platform.startswith("win"):
            deleted = force_delete_windows(key_path)

        if deleted and not key_path.exists():
            print(f"[OK] Local key file deleted: {key_path}")
        else:
            print(f"[WARN] Could not delete local key file (likely locked): {key_path}")
            print("       Close VSCode/PyCharm/terminals using it, or delete manually.")
