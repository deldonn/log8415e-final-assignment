"""
AWS client utilities and session management.
"""
import boto3
from botocore.exceptions import ClientError
from .config import get_config


def get_session():
    """Get boto3 session with configured region."""
    config = get_config()
    return boto3.Session(region_name=config["aws"]["region"])


def get_ec2_client():
    """Get EC2 client."""
    return get_session().client("ec2")


def get_ec2_resource():
    """Get EC2 resource."""
    return get_session().resource("ec2")


def get_project_tags() -> list:
    """Get standard project tags as list of dicts."""
    config = get_config()
    return [
        {"Key": "Project", "Value": config["tags"]["project"]},
        {"Key": "Environment", "Value": config["tags"]["environment"]},
    ]


def get_project_filters() -> list:
    """Get filters to find project resources."""
    config = get_config()
    return [
        {"Name": "tag:Project", "Values": [config["tags"]["project"]]},
    ]


def wait_for_instances_running(instance_ids: list, max_wait: int = 300):
    """Wait for instances to be in running state."""
    ec2 = get_ec2_client()
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(
        InstanceIds=instance_ids,
        WaiterConfig={"Delay": 10, "MaxAttempts": max_wait // 10}
    )


def wait_for_instances_terminated(instance_ids: list, max_wait: int = 300):
    """Wait for instances to be terminated."""
    ec2 = get_ec2_client()
    waiter = ec2.get_waiter("instance_terminated")
    waiter.wait(
        InstanceIds=instance_ids,
        WaiterConfig={"Delay": 10, "MaxAttempts": max_wait // 10}
    )






