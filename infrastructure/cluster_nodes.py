"""
Proxy and Gatekeeper nodes infrastructure management.
"""
import base64
import time
from pathlib import Path
from botocore.exceptions import ClientError
from .aws_client import (
    get_ec2_client, 
    get_ec2_resource, 
    get_project_tags, 
    get_project_filters,
    get_config,
    wait_for_instances_running,
    wait_for_instances_terminated
)
from .security_groups import (
    create_gatekeeper_security_group,
    create_proxy_security_group,
    get_security_group_id
)
from .keypair import create_key_pair
from .db_nodes import get_latest_ubuntu_ami


def get_minimal_user_data() -> str:
    """Generate minimal user-data for Proxy/Gatekeeper instances."""
    return """#!/bin/bash
exec > >(tee /var/log/user-data.log) 2>&1
echo "=== Instance initialized at $(date) ==="
# Wait for apt to be ready
while fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 1; done
while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do sleep 1; done
apt-get update -qq
apt-get install -y -qq python3-pip python3-venv
echo "=== Ready for remote setup ==="
"""


# =============================================================================
# Proxy Instance
# =============================================================================

def create_proxy_instance() -> dict:
    """
    Create the Proxy EC2 instance (t3.small).
    Returns instance info dict.
    """
    config = get_config()
    ec2 = get_ec2_client()
    
    # Prerequisites
    key_name = create_key_pair()
    
    # Get/create security groups in order
    gk_sg_id = create_gatekeeper_security_group()
    proxy_sg_id = create_proxy_security_group(gk_sg_id)
    
    ami_id = get_latest_ubuntu_ami()
    
    instance_type = config["instances"]["proxy"]["type"]
    instance_name = f"{config['tags']['project']}-proxy"
    
    user_data = get_minimal_user_data()
    user_data_b64 = base64.b64encode(user_data.encode()).decode()
    
    # Check if instance already exists
    existing = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [instance_name]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
        ] + get_project_filters()
    )
    
    if existing["Reservations"]:
        instance = existing["Reservations"][0]["Instances"][0]
        print(f"[OK] Proxy instance already exists: {instance['InstanceId']}")
        return {
            "role": "proxy",
            "instance_id": instance["InstanceId"],
            "private_ip": instance.get("PrivateIpAddress"),
            "public_ip": instance.get("PublicIpAddress"),
            "state": instance["State"]["Name"]
        }
    
    # Create instance
    print(f"[+] Creating Proxy instance ({instance_type})...")
    
    response = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        UserData=user_data_b64,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": get_project_tags() + [
                {"Key": "Name", "Value": instance_name},
                {"Key": "Role", "Value": "proxy"},
                {"Key": "Component", "Value": "proxy"}
            ]
        }],
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "AssociatePublicIpAddress": True,
            "Groups": [proxy_sg_id],
            "DeleteOnTermination": True
        }]
    )
    
    instance = response["Instances"][0]
    instance_id = instance["InstanceId"]
    print(f"[OK] Proxy instance created: {instance_id}")
    
    # Wait for running
    print("[...] Waiting for instance to be running...")
    wait_for_instances_running([instance_id])
    
    # Refresh to get IPs
    response = ec2.describe_instances(InstanceIds=[instance_id])
    instance = response["Reservations"][0]["Instances"][0]
    
    return {
        "role": "proxy",
        "instance_id": instance_id,
        "private_ip": instance.get("PrivateIpAddress"),
        "public_ip": instance.get("PublicIpAddress"),
        "state": instance["State"]["Name"]
    }


# =============================================================================
# Gatekeeper Instance
# =============================================================================

def create_gatekeeper_instance() -> dict:
    """
    Create the Gatekeeper EC2 instance (t3.small).
    Returns instance info dict.
    """
    config = get_config()
    ec2 = get_ec2_client()
    
    # Prerequisites
    key_name = create_key_pair()
    gk_sg_id = create_gatekeeper_security_group()
    ami_id = get_latest_ubuntu_ami()
    
    instance_type = config["instances"]["gatekeeper"]["type"]
    instance_name = f"{config['tags']['project']}-gatekeeper"
    
    user_data = get_minimal_user_data()
    user_data_b64 = base64.b64encode(user_data.encode()).decode()
    
    # Check if instance already exists
    existing = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [instance_name]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
        ] + get_project_filters()
    )
    
    if existing["Reservations"]:
        instance = existing["Reservations"][0]["Instances"][0]
        print(f"[OK] Gatekeeper instance already exists: {instance['InstanceId']}")
        return {
            "role": "gatekeeper",
            "instance_id": instance["InstanceId"],
            "private_ip": instance.get("PrivateIpAddress"),
            "public_ip": instance.get("PublicIpAddress"),
            "state": instance["State"]["Name"]
        }
    
    # Create instance
    print(f"[+] Creating Gatekeeper instance ({instance_type})...")
    
    response = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        UserData=user_data_b64,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": get_project_tags() + [
                {"Key": "Name", "Value": instance_name},
                {"Key": "Role", "Value": "gatekeeper"},
                {"Key": "Component", "Value": "gatekeeper"}
            ]
        }],
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "AssociatePublicIpAddress": True,
            "Groups": [gk_sg_id],
            "DeleteOnTermination": True
        }]
    )
    
    instance = response["Instances"][0]
    instance_id = instance["InstanceId"]
    print(f"[OK] Gatekeeper instance created: {instance_id}")
    
    # Wait for running
    print("[...] Waiting for instance to be running...")
    wait_for_instances_running([instance_id])
    
    # Refresh to get IPs
    response = ec2.describe_instances(InstanceIds=[instance_id])
    instance = response["Reservations"][0]["Instances"][0]
    
    return {
        "role": "gatekeeper",
        "instance_id": instance_id,
        "private_ip": instance.get("PrivateIpAddress"),
        "public_ip": instance.get("PublicIpAddress"),
        "state": instance["State"]["Name"]
    }


# =============================================================================
# Deploy both Proxy and Gatekeeper
# =============================================================================

def create_phase2_instances() -> dict:
    """
    Create both Proxy and Gatekeeper instances.
    Returns dict with instance info.
    """
    print("\n[Phase 2] Creating Proxy and Gatekeeper instances...")
    
    gatekeeper = create_gatekeeper_instance()
    proxy = create_proxy_instance()
    
    return {
        "proxy": proxy,
        "gatekeeper": gatekeeper
    }


# =============================================================================
# Status and Destroy
# =============================================================================

def get_proxy_gatekeeper_status() -> dict:
    """Get status of Proxy and Gatekeeper instances."""
    config = get_config()
    ec2 = get_ec2_client()
    
    result = {"proxy": None, "gatekeeper": None}
    
    for role in ["proxy", "gatekeeper"]:
        instance_name = f"{config['tags']['project']}-{role}"
        
        response = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
            ] + get_project_filters()
        )
        
        if response["Reservations"]:
            instance = response["Reservations"][0]["Instances"][0]
            result[role] = {
                "role": role,
                "instance_id": instance["InstanceId"],
                "instance_type": instance.get("InstanceType"),
                "private_ip": instance.get("PrivateIpAddress"),
                "public_ip": instance.get("PublicIpAddress"),
                "state": instance["State"]["Name"]
            }
    
    return result


def destroy_proxy_gatekeeper():
    """Terminate Proxy and Gatekeeper instances."""
    config = get_config()
    ec2 = get_ec2_client()
    
    instance_ids = []
    
    for role in ["proxy", "gatekeeper"]:
        instance_name = f"{config['tags']['project']}-{role}"
        
        response = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
            ] + get_project_filters()
        )
        
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                instance_ids.append(instance["InstanceId"])
    
    if not instance_ids:
        print("[OK] No Proxy/Gatekeeper instances to terminate")
        return
    
    print(f"[+] Terminating {len(instance_ids)} instance(s)...")
    ec2.terminate_instances(InstanceIds=instance_ids)
    
    print("[...] Waiting for instances to terminate...")
    wait_for_instances_terminated(instance_ids)
    print("[OK] Proxy and Gatekeeper instances terminated")


def print_phase2_status():
    """Print formatted status of Proxy and Gatekeeper."""
    status = get_proxy_gatekeeper_status()
    
    print("\n" + "=" * 70)
    print("PROXY & GATEKEEPER STATUS")
    print("=" * 70)
    
    for role in ["gatekeeper", "proxy"]:
        inst = status.get(role)
        if inst:
            print(f"\n  {role.upper()}")
            print(f"    Instance ID:  {inst['instance_id']}")
            print(f"    Type:         {inst['instance_type']}")
            print(f"    State:        {inst['state']}")
            print(f"    Private IP:   {inst['private_ip']}")
            print(f"    Public IP:    {inst['public_ip']}")
        else:
            print(f"\n  {role.upper()}: Not deployed")
    
    print("\n" + "=" * 70)
    
    # Show Gatekeeper endpoint if available
    gk = status.get("gatekeeper")
    if gk and gk.get("public_ip"):
        config = get_config()
        port = config["gatekeeper"]["port"]
        print(f"\nGatekeeper endpoint: http://{gk['public_ip']}:{port}")
    
    print()




