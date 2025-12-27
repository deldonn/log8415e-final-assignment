"""
DB nodes (MySQL cluster) infrastructure management.
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
from .security_groups import create_db_security_group, get_default_vpc_id
from .keypair import create_key_pair


def get_latest_ubuntu_ami() -> str:
    """Get the latest Ubuntu 22.04 LTS AMI ID."""
    config = get_config()
    ec2 = get_ec2_client()
    
    response = ec2.describe_images(
        Filters=[
            {"Name": "name", "Values": [config["instances"]["db"]["ami_name_pattern"]]},
            {"Name": "state", "Values": ["available"]},
            {"Name": "architecture", "Values": ["x86_64"]},
        ],
        Owners=[config["instances"]["db"]["ami_owner"]]
    )
    
    if not response["Images"]:
        raise RuntimeError("No Ubuntu AMI found matching the pattern")
    
    # Sort by creation date and get the latest
    images = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
    ami_id = images[0]["ImageId"]
    print(f"[OK] Found Ubuntu AMI: {ami_id} ({images[0]['Name']})")
    return ami_id


def get_db_user_data() -> str:
    """
    Generate minimal user-data script for DB nodes.
    Full setup is done via SSH from Python (setup_db.py).
    """
    # Minimal user-data - just ensure system is ready
    user_data = """#!/bin/bash
exec > >(tee /var/log/user-data.log) 2>&1
echo "=== Instance initialized at $(date) ==="
echo "Waiting for cloud-init to complete system setup..."
# The actual MySQL/Sakila setup will be done via SSH from Python
echo "=== Ready for remote setup ==="
"""
    return user_data


def create_db_nodes() -> list:
    """
    Create the 3 DB node instances (manager + 2 workers).
    Returns list of instance dictionaries.
    """
    config = get_config()
    ec2 = get_ec2_client()
    
    # Prerequisites
    key_name = create_key_pair()
    sg_id = create_db_security_group()
    ami_id = get_latest_ubuntu_ami()
    
    db_config = config["instances"]["db"]
    roles = db_config["roles"]
    instance_type = db_config["type"]
    
    user_data = get_db_user_data()
    user_data_b64 = base64.b64encode(user_data.encode()).decode()
    
    created_instances = []
    
    for i, role in enumerate(roles):
        instance_name = f"{config['tags']['project']}-db-{role}"
        
        # Check if instance already exists
        existing = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
            ] + get_project_filters()
        )
        
        if existing["Reservations"]:
            instance = existing["Reservations"][0]["Instances"][0]
            print(f"[OK] DB node '{role}' already exists: {instance['InstanceId']}")
            created_instances.append({
                "role": role,
                "instance_id": instance["InstanceId"],
                "private_ip": instance.get("PrivateIpAddress"),
                "public_ip": instance.get("PublicIpAddress"),
                "state": instance["State"]["Name"]
            })
            continue
        
        # Create instance
        print(f"[+] Creating DB node '{role}' ({instance_type})...")
        
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
                    {"Key": "Role", "Value": role},
                    {"Key": "Component", "Value": "db"}
                ]
            }],
            # NetworkInterfaces with public IP and security group
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "Groups": [sg_id],
                "DeleteOnTermination": True
            }]
        )
        
        instance = response["Instances"][0]
        created_instances.append({
            "role": role,
            "instance_id": instance["InstanceId"],
            "private_ip": None,  # Will be assigned after running
            "public_ip": None,
            "state": instance["State"]["Name"]
        })
        print(f"[OK] DB node '{role}' created: {instance['InstanceId']}")
    
    # Wait for all instances to be running
    instance_ids = [inst["instance_id"] for inst in created_instances]
    pending_ids = [
        inst["instance_id"] for inst in created_instances 
        if inst["state"] != "running"
    ]
    
    if pending_ids:
        print(f"[...] Waiting for {len(pending_ids)} instance(s) to be running...")
        wait_for_instances_running(pending_ids)
        print("[OK] All instances are running")
    
    # Refresh instance info to get IPs
    response = ec2.describe_instances(InstanceIds=instance_ids)
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            for inst in created_instances:
                if inst["instance_id"] == instance["InstanceId"]:
                    inst["private_ip"] = instance.get("PrivateIpAddress")
                    inst["public_ip"] = instance.get("PublicIpAddress")
                    inst["state"] = instance["State"]["Name"]
    
    return created_instances


def get_db_nodes_status() -> list:
    """Get status of all DB node instances."""
    config = get_config()
    ec2 = get_ec2_client()
    
    response = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Component", "Values": ["db"]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
        ] + get_project_filters()
    )
    
    instances = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            # Get role from tags
            role = "unknown"
            for tag in instance.get("Tags", []):
                if tag["Key"] == "Role":
                    role = tag["Value"]
                    break
            
            instances.append({
                "role": role,
                "instance_id": instance["InstanceId"],
                "private_ip": instance.get("PrivateIpAddress"),
                "public_ip": instance.get("PublicIpAddress"),
                "state": instance["State"]["Name"],
                "instance_type": instance.get("InstanceType")
            })
    
    # Sort by role
    role_order = {"manager": 0, "worker1": 1, "worker2": 2}
    instances.sort(key=lambda x: role_order.get(x["role"], 99))
    
    return instances


def destroy_db_nodes():
    """Terminate all DB node instances."""
    config = get_config()
    ec2 = get_ec2_client()
    
    # Find all DB instances
    response = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Component", "Values": ["db"]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
        ] + get_project_filters()
    )
    
    instance_ids = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            instance_ids.append(instance["InstanceId"])
    
    if not instance_ids:
        print("[OK] No DB instances to terminate")
        return
    
    print(f"[+] Terminating {len(instance_ids)} DB instance(s)...")
    ec2.terminate_instances(InstanceIds=instance_ids)
    
    print("[...] Waiting for instances to terminate...")
    wait_for_instances_terminated(instance_ids)
    print("[OK] All DB instances terminated")


def print_db_status():
    """Print formatted status of DB nodes."""
    instances = get_db_nodes_status()
    
    if not instances:
        print("\n[!] No DB instances found")
        return
    
    print("\n" + "=" * 70)
    print("DB NODES STATUS")
    print("=" * 70)
    
    for inst in instances:
        print(f"\n  {inst['role'].upper()}")
        print(f"    Instance ID:  {inst['instance_id']}")
        print(f"    Type:         {inst['instance_type']}")
        print(f"    State:        {inst['state']}")
        print(f"    Private IP:   {inst['private_ip']}")
        print(f"    Public IP:    {inst['public_ip']}")
    
    print("\n" + "=" * 70)
    
    # SSH command helper
    if instances and instances[0].get("public_ip"):
        from .keypair import get_key_path
        key_path = get_key_path()
        print("\nSSH Commands:")
        for inst in instances:
            if inst.get("public_ip"):
                print(f"  ssh -i {key_path} ubuntu@{inst['public_ip']}  # {inst['role']}")
    print()

