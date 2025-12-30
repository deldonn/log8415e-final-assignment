"""
DB Nodes Infrastructure - LOG8415E Final Assignment

Manages the MySQL cluster EC2 instances:
- 1 Manager (master) node
- 2 Worker (replica) nodes

All nodes are t3.micro instances running Ubuntu 22.04 with MySQL 8.
"""
import base64
from .aws_client import (
    get_ec2_client,
    get_project_tags,
    get_project_filters,
    get_config,
    wait_for_instances_running,
    wait_for_instances_terminated
)
from .security_groups import create_db_security_group
from .keypair import create_key_pair


# =============================================================================
# AMI and User Data
# =============================================================================

def get_latest_ubuntu_ami() -> str:
    """
    Get the latest Ubuntu 22.04 LTS AMI ID for the region.
    
    Searches AWS for the most recent Ubuntu Jammy AMI owned by Canonical.
    The AMI pattern is configured in settings.yaml.
    
    Returns:
        AMI ID string (e.g., 'ami-0123456789abcdef0')
    
    Raises:
        RuntimeError: If no matching AMI is found
    """
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
    print(f"[OK] Found Ubuntu AMI: {ami_id}")
    return ami_id


def get_db_user_data() -> str:
    """
    Generate minimal user-data script for DB nodes.
    
    Only logs initialization - actual MySQL setup is done via SSH
    from setup_db.py for better error handling and logging.
    
    Returns:
        Bash script as string
    """
    return """#!/bin/bash
exec > >(tee /var/log/user-data.log) 2>&1
echo "=== Instance initialized at $(date) ==="
echo "Ready for remote MySQL setup via SSH"
"""


# =============================================================================
# Instance Creation
# =============================================================================

def create_db_nodes() -> list:
    """
    Create the 3 DB node EC2 instances (1 manager + 2 workers).
    
    Process:
    1. Create/get SSH key pair
    2. Create/get security group
    3. Find latest Ubuntu AMI
    4. Create instances with proper tags
    5. Wait for instances to be running
    6. Return instance info with IPs
    
    Skips creation if instances already exist (idempotent).
    
    Returns:
        List of instance dicts with keys:
        - role: 'manager', 'worker1', or 'worker2'
        - instance_id: EC2 instance ID
        - private_ip: Private IP address
        - public_ip: Public IP address
        - state: Instance state
    """
    config = get_config()
    ec2 = get_ec2_client()
    
    # Prerequisites
    key_name = create_key_pair()
    sg_id = create_db_security_group()
    ami_id = get_latest_ubuntu_ami()
    
    db_config = config["instances"]["db"]
    roles = db_config["roles"]  # ['manager', 'worker1', 'worker2']
    instance_type = db_config["type"]  # 't3.micro'
    
    user_data_b64 = base64.b64encode(get_db_user_data().encode()).decode()
    
    created_instances = []
    
    for role in roles:
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
        
        # Create new instance
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
            "private_ip": None,
            "public_ip": None,
            "state": instance["State"]["Name"]
        })
        print(f"[OK] DB node '{role}' created: {instance['InstanceId']}")
    
    # Wait for pending instances to be running
    pending_ids = [i["instance_id"] for i in created_instances if i["state"] != "running"]
    
    if pending_ids:
        print(f"[...] Waiting for {len(pending_ids)} instance(s) to be running...")
        wait_for_instances_running(pending_ids)
        print("[OK] All instances are running")
    
    # Refresh instance info to get IPs
    instance_ids = [i["instance_id"] for i in created_instances]
    response = ec2.describe_instances(InstanceIds=instance_ids)
    
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            for inst in created_instances:
                if inst["instance_id"] == instance["InstanceId"]:
                    inst["private_ip"] = instance.get("PrivateIpAddress")
                    inst["public_ip"] = instance.get("PublicIpAddress")
                    inst["state"] = instance["State"]["Name"]
    
    return created_instances


# =============================================================================
# Status and Destruction
# =============================================================================

def get_db_nodes_status() -> list:
    """
    Get current status of all DB node instances.
    
    Queries AWS for instances tagged with Component=db.
    
    Returns:
        List of instance dicts sorted by role (manager, worker1, worker2)
        Each dict contains: role, instance_id, private_ip, public_ip, state, instance_type
    """
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
    """
    Terminate all DB node EC2 instances.
    
    Finds instances by Component=db tag and terminates them.
    Waits for termination to complete before returning.
    """
    ec2 = get_ec2_client()
    
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
    """
    Print formatted status of all DB nodes.
    
    Displays instance details and SSH commands for easy access.
    """
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
