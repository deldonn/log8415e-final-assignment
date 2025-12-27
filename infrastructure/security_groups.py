"""
Security Groups management for the project.
"""
from botocore.exceptions import ClientError
from .aws_client import get_ec2_client, get_ec2_resource, get_project_tags, get_config


def get_default_vpc_id() -> str:
    """Get the default VPC ID."""
    ec2 = get_ec2_client()
    response = ec2.describe_vpcs(
        Filters=[{"Name": "is-default", "Values": ["true"]}]
    )
    if not response["Vpcs"]:
        raise RuntimeError("No default VPC found. Please create one or specify a VPC ID.")
    return response["Vpcs"][0]["VpcId"]


def _get_existing_sg(ec2, sg_name: str, vpc_id: str) -> str:
    """
    Check if a security group already exists.
    Returns the security group ID if found, None otherwise.
    """
    try:
        response = ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [sg_name]},
                {"Name": "vpc-id", "Values": [vpc_id]}
            ]
        )
        if response["SecurityGroups"]:
            return response["SecurityGroups"][0]["GroupId"]
    except ClientError:
        pass
    return None


def _ensure_ssh_rule(ec2, sg_id: str):
    """
    Ensure SSH from 0.0.0.0/0 rule exists in the security group.
    This is needed for initial setup via SSH from local machine.
    """
    try:
        response = ec2.describe_security_groups(GroupIds=[sg_id])
        sg = response["SecurityGroups"][0]
        
        # Check if SSH rule from 0.0.0.0/0 already exists
        ssh_open = False
        for perm in sg.get("IpPermissions", []):
            if perm.get("FromPort") == 22 and perm.get("ToPort") == 22:
                for ip_range in perm.get("IpRanges", []):
                    if ip_range.get("CidrIp") == "0.0.0.0/0":
                        ssh_open = True
                        break
        
        if not ssh_open:
            print(f"[+] Adding SSH rule (0.0.0.0/0) to {sg_id}...")
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
                }]
            )
            print(f"[OK] SSH rule added")
            
    except ClientError as e:
        if "InvalidPermission.Duplicate" in str(e):
            pass  # Rule already exists
        else:
            print(f"[WARN] Could not add SSH rule: {e}")


# =============================================================================
# DB Security Group
# =============================================================================

def create_db_security_group() -> str:
    """
    Create security group for DB nodes.
    - MySQL port 3306: open to VPC CIDR (for Proxy access)
    - SSH port 22: open to 0.0.0.0/0 (for initial setup)
    - All traffic between DB nodes (for replication)
    Returns the security group ID.
    """
    config = get_config()
    ec2 = get_ec2_client()
    ec2_resource = get_ec2_resource()
    
    sg_name = f"{config['tags']['project']}-db-sg"
    vpc_id = get_default_vpc_id()
    vpc_cidr = ec2_resource.Vpc(vpc_id).cidr_block

    # Check if SG already exists
    existing_sg = _get_existing_sg(ec2, sg_name, vpc_id)
    if existing_sg:
        print(f"[OK] DB security group already exists: {existing_sg}")
        _ensure_ssh_rule(ec2, existing_sg)
        return existing_sg

    # Create security group
    print(f"[+] Creating DB security group '{sg_name}'...")
    response = ec2.create_security_group(
        GroupName=sg_name,
        Description="Security group for MySQL DB nodes - LOG8415E",
        VpcId=vpc_id,
        TagSpecifications=[{
            "ResourceType": "security-group",
            "Tags": get_project_tags() + [{"Key": "Name", "Value": sg_name}]
        }]
    )
    sg_id = response["GroupId"]

    # Add ingress rules
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            # MySQL from VPC (Proxy will connect here)
            {
                "IpProtocol": "tcp",
                "FromPort": 3306,
                "ToPort": 3306,
                "IpRanges": [{"CidrIp": vpc_cidr, "Description": "MySQL from VPC"}]
            },
            # SSH from anywhere (for initial setup)
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
            },
            # All traffic between DB nodes (replication)
            {
                "IpProtocol": "-1",
                "UserIdGroupPairs": [{"GroupId": sg_id, "Description": "DB nodes replication"}]
            }
        ]
    )

    print(f"[OK] DB security group created: {sg_id}")
    return sg_id


# =============================================================================
# Proxy Security Group
# =============================================================================

def create_proxy_security_group() -> str:
    """
    Create security group for Proxy.
    - Port 8000: open to VPC CIDR (Gatekeeper connects here)
    - SSH port 22: open to 0.0.0.0/0 (for initial setup)
    Returns the security group ID.
    """
    config = get_config()
    ec2 = get_ec2_client()
    ec2_resource = get_ec2_resource()
    
    sg_name = f"{config['tags']['project']}-proxy-sg"
    vpc_id = get_default_vpc_id()
    vpc_cidr = ec2_resource.Vpc(vpc_id).cidr_block

    # Check if SG already exists
    existing_sg = _get_existing_sg(ec2, sg_name, vpc_id)
    if existing_sg:
        print(f"[OK] Proxy security group already exists: {existing_sg}")
        _ensure_ssh_rule(ec2, existing_sg)
        return existing_sg

    # Create security group
    print(f"[+] Creating Proxy security group '{sg_name}'...")
    response = ec2.create_security_group(
        GroupName=sg_name,
        Description="Security group for Proxy - LOG8415E",
        VpcId=vpc_id,
        TagSpecifications=[{
            "ResourceType": "security-group",
            "Tags": get_project_tags() + [{"Key": "Name", "Value": sg_name}]
        }]
    )
    sg_id = response["GroupId"]

    # Add ingress rules
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            # Proxy port from VPC (Gatekeeper connects here)
            {
                "IpProtocol": "tcp",
                "FromPort": config["proxy"]["port"],
                "ToPort": config["proxy"]["port"],
                "IpRanges": [{"CidrIp": vpc_cidr, "Description": "Proxy port from VPC"}]
            },
            # SSH from anywhere (for initial setup)
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
            }
        ]
    )

    print(f"[OK] Proxy security group created: {sg_id}")
    return sg_id


# =============================================================================
# Gatekeeper Security Group
# =============================================================================

def create_gatekeeper_security_group() -> str:
    """
    Create security group for Gatekeeper.
    - Port 8080: open to Internet (0.0.0.0/0) - public facing
    - SSH port 22: open to 0.0.0.0/0 (for initial setup)
    Returns the security group ID.
    """
    config = get_config()
    ec2 = get_ec2_client()
    ec2_resource = get_ec2_resource()
    
    sg_name = f"{config['tags']['project']}-gatekeeper-sg"
    vpc_id = get_default_vpc_id()

    # Check if SG already exists
    existing_sg = _get_existing_sg(ec2, sg_name, vpc_id)
    if existing_sg:
        print(f"[OK] Gatekeeper security group already exists: {existing_sg}")
        _ensure_ssh_rule(ec2, existing_sg)
        return existing_sg

    # Create security group
    print(f"[+] Creating Gatekeeper security group '{sg_name}'...")
    response = ec2.create_security_group(
        GroupName=sg_name,
        Description="Security group for Gatekeeper - LOG8415E",
        VpcId=vpc_id,
        TagSpecifications=[{
            "ResourceType": "security-group",
            "Tags": get_project_tags() + [{"Key": "Name", "Value": sg_name}]
        }]
    )
    sg_id = response["GroupId"]

    # Add ingress rules
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            # Gatekeeper port from Internet (public facing)
            {
                "IpProtocol": "tcp",
                "FromPort": config["gatekeeper"]["port"],
                "ToPort": config["gatekeeper"]["port"],
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Gatekeeper public port"}]
            },
            # SSH from anywhere (for initial setup)
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
            }
        ]
    )

    print(f"[OK] Gatekeeper security group created: {sg_id}")
    return sg_id


# =============================================================================
# Cleanup
# =============================================================================

def delete_security_groups():
    """Delete all project security groups."""
    config = get_config()
    ec2 = get_ec2_client()
    vpc_id = get_default_vpc_id()
    
    sg_names = [
        f"{config['tags']['project']}-db-sg",
        f"{config['tags']['project']}-proxy-sg",
        f"{config['tags']['project']}-gatekeeper-sg",
    ]

    for sg_name in sg_names:
        sg_id = _get_existing_sg(ec2, sg_name, vpc_id)
        if sg_id:
            try:
                ec2.delete_security_group(GroupId=sg_id)
                print(f"[OK] Deleted: {sg_name}")
            except ClientError as e:
                if "DependencyViolation" in str(e):
                    print(f"[WARN] Cannot delete {sg_name} - still in use")
                else:
                    raise
        else:
            print(f"[OK] Not found: {sg_name}")


def get_security_group_id(sg_name: str) -> str:
    """Get security group ID by name."""
    ec2 = get_ec2_client()
    vpc_id = get_default_vpc_id()
    return _get_existing_sg(ec2, sg_name, vpc_id)
