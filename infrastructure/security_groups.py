"""
Security Groups - LOG8415E Final Assignment

Manages AWS Security Groups for the Gatekeeper pattern:
- Gatekeeper SG: Public-facing, accepts traffic from Internet
- Proxy SG: Internal, accepts traffic ONLY from Gatekeeper SG
- DB SG: Internal, accepts traffic ONLY from Proxy SG

This implements strict SG-to-SG rules for defense in depth.
"""
from botocore.exceptions import ClientError
from .aws_client import get_ec2_client, get_ec2_resource, get_project_tags, get_config


# =============================================================================
# VPC and Security Group Helpers
# =============================================================================

def get_default_vpc_id() -> str:
    """
    Get the default VPC ID for the AWS account.
    
    Returns:
        VPC ID string
    
    Raises:
        RuntimeError: If no default VPC exists
    """
    ec2 = get_ec2_client()
    response = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
    
    if not response["Vpcs"]:
        raise RuntimeError("No default VPC found")
    
    return response["Vpcs"][0]["VpcId"]


def _get_existing_sg(ec2, sg_name: str, vpc_id: str) -> str:
    """
    Check if a security group already exists.
    
    Args:
        ec2: EC2 client
        sg_name: Name of the security group
        vpc_id: VPC ID to search in
    
    Returns:
        Security group ID if found, None otherwise
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
    Ensure SSH access rule exists in the security group.
    
    Adds SSH (port 22) from 0.0.0.0/0 if not already present.
    Required for initial setup via SSH from local machine.
    
    Args:
        ec2: EC2 client
        sg_id: Security group ID
    """
    try:
        response = ec2.describe_security_groups(GroupIds=[sg_id])
        sg = response["SecurityGroups"][0]
        
        # Check if SSH rule already exists
        ssh_open = any(
            perm.get("FromPort") == 22 and perm.get("ToPort") == 22
            and any(ip.get("CidrIp") == "0.0.0.0/0" for ip in perm.get("IpRanges", []))
            for perm in sg.get("IpPermissions", [])
        )
        
        if not ssh_open:
            print(f"[+] Adding SSH rule to {sg_id}...")
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
                }]
            )
    except ClientError as e:
        if "InvalidPermission.Duplicate" not in str(e):
            print(f"[WARN] Could not add SSH rule: {e}")


def _add_sg_rule_if_missing(ec2, sg_id: str, port: int, source_sg_id: str, description: str):
    """
    Add a security group rule allowing traffic from another SG.
    
    This creates SG-to-SG rules which are more secure than IP-based rules.
    
    Args:
        ec2: EC2 client
        sg_id: Target security group ID
        port: Port to allow
        source_sg_id: Source security group ID
        description: Rule description
    """
    try:
        response = ec2.describe_security_groups(GroupIds=[sg_id])
        sg = response["SecurityGroups"][0]
        
        # Check if rule already exists
        rule_exists = any(
            perm.get("FromPort") == port and perm.get("ToPort") == port
            and any(pair.get("GroupId") == source_sg_id for pair in perm.get("UserIdGroupPairs", []))
            for perm in sg.get("IpPermissions", [])
        )
        
        if not rule_exists:
            print(f"[+] Adding rule: port {port} from {source_sg_id}...")
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": port,
                    "ToPort": port,
                    "UserIdGroupPairs": [{"GroupId": source_sg_id, "Description": description}]
                }]
            )
    except ClientError as e:
        if "InvalidPermission.Duplicate" not in str(e):
            print(f"[WARN] Could not add rule: {e}")


def get_sg_names() -> dict:
    """
    Get security group names for the project.
    
    Names are prefixed with project name from config.
    
    Returns:
        Dict with 'db', 'proxy', 'gatekeeper' keys and SG names as values
    """
    config = get_config()
    project = config['tags']['project']
    return {
        "db": f"{project}-db-sg",
        "proxy": f"{project}-proxy-sg",
        "gatekeeper": f"{project}-gatekeeper-sg",
    }


# =============================================================================
# Gatekeeper Security Group (Public-facing)
# =============================================================================

def create_gatekeeper_security_group() -> str:
    """
    Create security group for Gatekeeper instance.
    
    Rules:
    - Port 8080: Open to Internet (0.0.0.0/0) - public API endpoint
    - Port 22: Open to Internet - SSH for initial setup
    
    This is the ONLY component exposed to the Internet.
    
    Returns:
        Security group ID
    """
    config = get_config()
    ec2 = get_ec2_client()
    sg_names = get_sg_names()
    sg_name = sg_names["gatekeeper"]
    vpc_id = get_default_vpc_id()

    # Return existing SG if found
    existing_sg = _get_existing_sg(ec2, sg_name, vpc_id)
    if existing_sg:
        print(f"[OK] Gatekeeper SG exists: {existing_sg}")
        _ensure_ssh_rule(ec2, existing_sg)
        return existing_sg

    # Create new security group
    print(f"[+] Creating Gatekeeper SG '{sg_name}'...")
    response = ec2.create_security_group(
        GroupName=sg_name,
        Description="Gatekeeper - public facing (LOG8415E)",
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
            {
                "IpProtocol": "tcp",
                "FromPort": config["gatekeeper"]["port"],
                "ToPort": config["gatekeeper"]["port"],
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Gatekeeper API"}]
            },
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
            }
        ]
    )

    print(f"[OK] Gatekeeper SG created: {sg_id}")
    return sg_id


# =============================================================================
# Proxy Security Group (Internal - Gatekeeper only)
# =============================================================================

def create_proxy_security_group(gatekeeper_sg_id: str = None) -> str:
    """
    Create security group for Proxy instance.
    
    Rules:
    - Port 8000: Open ONLY from Gatekeeper SG (strict SG-to-SG rule)
    - Port 22: Open to Internet - SSH for initial setup
    
    The Proxy is NOT accessible from the Internet directly.
    
    Args:
        gatekeeper_sg_id: Gatekeeper SG ID (auto-detected if not provided)
    
    Returns:
        Security group ID
    """
    config = get_config()
    ec2 = get_ec2_client()
    sg_names = get_sg_names()
    sg_name = sg_names["proxy"]
    vpc_id = get_default_vpc_id()

    # Auto-detect Gatekeeper SG if not provided
    if not gatekeeper_sg_id:
        gatekeeper_sg_id = _get_existing_sg(ec2, sg_names["gatekeeper"], vpc_id)
    
    # Return existing SG if found
    existing_sg = _get_existing_sg(ec2, sg_name, vpc_id)
    if existing_sg:
        print(f"[OK] Proxy SG exists: {existing_sg}")
        _ensure_ssh_rule(ec2, existing_sg)
        if gatekeeper_sg_id:
            _add_sg_rule_if_missing(ec2, existing_sg, config["proxy"]["port"], 
                                     gatekeeper_sg_id, "Proxy from Gatekeeper")
        return existing_sg

    # Create new security group
    print(f"[+] Creating Proxy SG '{sg_name}'...")
    response = ec2.create_security_group(
        GroupName=sg_name,
        Description="Proxy - Gatekeeper only (LOG8415E)",
        VpcId=vpc_id,
        TagSpecifications=[{
            "ResourceType": "security-group",
            "Tags": get_project_tags() + [{"Key": "Name", "Value": sg_name}]
        }]
    )
    sg_id = response["GroupId"]

    # Build ingress rules
    ip_permissions = [{
        "IpProtocol": "tcp",
        "FromPort": 22,
        "ToPort": 22,
        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
    }]
    
    if gatekeeper_sg_id:
        # Strict SG-to-SG rule
        ip_permissions.append({
            "IpProtocol": "tcp",
            "FromPort": config["proxy"]["port"],
            "ToPort": config["proxy"]["port"],
            "UserIdGroupPairs": [{"GroupId": gatekeeper_sg_id, "Description": "From Gatekeeper"}]
        })
    else:
        # Fallback to VPC CIDR (less secure)
        vpc_cidr = get_ec2_resource().Vpc(vpc_id).cidr_block
        ip_permissions.append({
            "IpProtocol": "tcp",
            "FromPort": config["proxy"]["port"],
            "ToPort": config["proxy"]["port"],
            "IpRanges": [{"CidrIp": vpc_cidr, "Description": "VPC fallback"}]
        })
        print(f"[WARN] Using VPC CIDR fallback (Gatekeeper SG not found)")

    ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=ip_permissions)
    print(f"[OK] Proxy SG created: {sg_id}")
    return sg_id


# =============================================================================
# DB Security Group (Internal - Proxy only)
# =============================================================================

def create_db_security_group(proxy_sg_id: str = None) -> str:
    """
    Create security group for DB (MySQL) instances.
    
    Rules:
    - Port 3306: Open ONLY from Proxy SG (strict SG-to-SG rule)
    - Port 22: Open to Internet - SSH for initial setup
    - All traffic: Between DB nodes (for replication)
    
    DB nodes are NOT accessible from the Internet or Gatekeeper.
    
    Args:
        proxy_sg_id: Proxy SG ID (auto-detected if not provided)
    
    Returns:
        Security group ID
    """
    ec2 = get_ec2_client()
    sg_names = get_sg_names()
    sg_name = sg_names["db"]
    vpc_id = get_default_vpc_id()

    # Auto-detect Proxy SG if not provided
    if not proxy_sg_id:
        proxy_sg_id = _get_existing_sg(ec2, sg_names["proxy"], vpc_id)

    # Return existing SG if found
    existing_sg = _get_existing_sg(ec2, sg_name, vpc_id)
    if existing_sg:
        print(f"[OK] DB SG exists: {existing_sg}")
        _ensure_ssh_rule(ec2, existing_sg)
        if proxy_sg_id:
            _add_sg_rule_if_missing(ec2, existing_sg, 3306, proxy_sg_id, "MySQL from Proxy")
        return existing_sg

    # Create new security group
    print(f"[+] Creating DB SG '{sg_name}'...")
    response = ec2.create_security_group(
        GroupName=sg_name,
        Description="MySQL DB nodes - Proxy only (LOG8415E)",
        VpcId=vpc_id,
        TagSpecifications=[{
            "ResourceType": "security-group",
            "Tags": get_project_tags() + [{"Key": "Name", "Value": sg_name}]
        }]
    )
    sg_id = response["GroupId"]

    # Build ingress rules
    ip_permissions = [
        {
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH for setup"}]
        },
        {
            # Allow all traffic between DB nodes (replication)
            "IpProtocol": "-1",
            "UserIdGroupPairs": [{"GroupId": sg_id, "Description": "DB replication"}]
        }
    ]
    
    if proxy_sg_id:
        # Strict SG-to-SG rule
        ip_permissions.append({
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "UserIdGroupPairs": [{"GroupId": proxy_sg_id, "Description": "MySQL from Proxy"}]
        })
    else:
        # Fallback to VPC CIDR (less secure)
        vpc_cidr = get_ec2_resource().Vpc(vpc_id).cidr_block
        ip_permissions.append({
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "IpRanges": [{"CidrIp": vpc_cidr, "Description": "VPC fallback"}]
        })
        print(f"[WARN] Using VPC CIDR fallback (Proxy SG not found)")

    ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=ip_permissions)
    print(f"[OK] DB SG created: {sg_id}")
    return sg_id


# =============================================================================
# Main Entry Points
# =============================================================================

def create_all_security_groups() -> dict:
    """
    Create all security groups in dependency order.
    
    Order:
    1. Gatekeeper (no dependencies)
    2. Proxy (depends on Gatekeeper SG)
    3. DB (depends on Proxy SG)
    
    Returns:
        Dict with 'gatekeeper', 'proxy', 'db' SG IDs
    """
    print("\n[SG] Creating security groups...")
    
    gatekeeper_sg = create_gatekeeper_security_group()
    proxy_sg = create_proxy_security_group(gatekeeper_sg)
    db_sg = create_db_security_group(proxy_sg)
    
    return {"gatekeeper": gatekeeper_sg, "proxy": proxy_sg, "db": db_sg}


def update_existing_sgs_strict_rules():
    """
    Update existing security groups with strict SG-to-SG rules.
    
    Use this to migrate from VPC CIDR rules to SG-based rules
    for improved security.
    
    Returns:
        Dict with SG IDs
    """
    ec2 = get_ec2_client()
    vpc_id = get_default_vpc_id()
    sg_names = get_sg_names()
    config = get_config()
    
    print("\n[SG] Updating with strict rules...")
    
    gk_sg = _get_existing_sg(ec2, sg_names["gatekeeper"], vpc_id)
    proxy_sg = _get_existing_sg(ec2, sg_names["proxy"], vpc_id)
    db_sg = _get_existing_sg(ec2, sg_names["db"], vpc_id)
    
    if proxy_sg and gk_sg:
        _add_sg_rule_if_missing(ec2, proxy_sg, config["proxy"]["port"], gk_sg, "From Gatekeeper")
        print(f"[OK] Proxy SG updated")
    
    if db_sg and proxy_sg:
        _add_sg_rule_if_missing(ec2, db_sg, 3306, proxy_sg, "MySQL from Proxy")
        print(f"[OK] DB SG updated")
    
    return {"gatekeeper": gk_sg, "proxy": proxy_sg, "db": db_sg}


def delete_security_groups():
    """
    Delete all project security groups.
    
    Deletes in reverse dependency order:
    1. DB (no dependents)
    2. Proxy (DB depends on it)
    3. Gatekeeper (Proxy depends on it)
    """
    ec2 = get_ec2_client()
    vpc_id = get_default_vpc_id()
    sg_names = get_sg_names()
    
    for sg_key in ["db", "proxy", "gatekeeper"]:
        sg_name = sg_names[sg_key]
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


def get_security_group_id(sg_type: str) -> str:
    """
    Get security group ID by type.
    
    Args:
        sg_type: One of 'db', 'proxy', 'gatekeeper'
    
    Returns:
        Security group ID or None if not found
    
    Raises:
        ValueError: If sg_type is invalid
    """
    sg_names = get_sg_names()
    
    if sg_type not in sg_names:
        raise ValueError(f"Unknown SG type: {sg_type}. Use 'db', 'proxy', or 'gatekeeper'")
    
    ec2 = get_ec2_client()
    vpc_id = get_default_vpc_id()
    return _get_existing_sg(ec2, sg_names[sg_type], vpc_id)
