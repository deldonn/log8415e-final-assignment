"""
EC2 Key Pair management.
"""
import os
from pathlib import Path
from botocore.exceptions import ClientError
from .aws_client import get_ec2_client, get_config


def get_key_path() -> Path:
    """Get path to store the private key."""
    config = get_config()
    key_name = config["aws"]["key_pair_name"]
    # Store in user's .ssh directory
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(exist_ok=True)
    return ssh_dir / f"{key_name}.pem"


def create_key_pair() -> str:
    """
    Create EC2 key pair if it doesn't exist.
    Returns the key pair name.
    """
    config = get_config()
    ec2 = get_ec2_client()
    key_name = config["aws"]["key_pair_name"]
    key_path = get_key_path()

    # Check if key pair exists in AWS
    try:
        ec2.describe_key_pairs(KeyNames=[key_name])
        print(f"[OK] Key pair '{key_name}' already exists in AWS")
        
        # Check if we have the private key locally
        if not key_path.exists():
            print(f"[WARN] Private key not found at {key_path}")
            print("       You may need to delete the key pair and recreate it.")
        return key_name
        
    except ClientError as e:
        if "InvalidKeyPair.NotFound" not in str(e):
            raise

    # Create new key pair
    print(f"[+] Creating key pair '{key_name}'...")
    response = ec2.create_key_pair(
        KeyName=key_name,
        KeyType="rsa",
        KeyFormat="pem"
    )

    # Save private key
    private_key = response["KeyMaterial"]
    with open(key_path, "w") as f:
        f.write(private_key)
    
    # Set permissions (Unix-like systems)
    try:
        os.chmod(key_path, 0o400)
    except Exception:
        pass  # Windows doesn't support chmod

    print(f"[OK] Key pair created, private key saved to: {key_path}")
    return key_name


def delete_key_pair():
    """Delete the EC2 key pair."""
    config = get_config()
    ec2 = get_ec2_client()
    key_name = config["aws"]["key_pair_name"]
    key_path = get_key_path()

    try:
        ec2.delete_key_pair(KeyName=key_name)
        print(f"[OK] Key pair '{key_name}' deleted from AWS")
    except ClientError as e:
        if "InvalidKeyPair.NotFound" not in str(e):
            raise
        print(f"[OK] Key pair '{key_name}' not found (already deleted)")

    # Remove local key file
    if key_path.exists():
        key_path.unlink()
        print(f"[OK] Local key file deleted: {key_path}")


