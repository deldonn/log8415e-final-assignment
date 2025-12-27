#!/usr/bin/env python3
"""
=============================================================================
LOG8415E Final Assignment - Full Automation Script
=============================================================================

This script automates the complete deployment of the cloud architecture:
- Phase 1: DB Cluster (3x t3.micro with MySQL + Sakila)
- Phase 2: Proxy (t3.small) + Gatekeeper (t3.small) + Replication
- Phase 3: Benchmarks (1000 reads + 1000 writes per strategy)

Usage:
    python deploy.py                 # Full deployment
    python deploy.py --phase 1       # Only Phase 1
    python deploy.py --destroy       # Cleanup all resources
    python deploy.py --status        # Show current status
"""
import sys
import time
import argparse
import json
from datetime import datetime
from pathlib import Path


def print_banner(text: str, char: str = "="):
    """Print a banner with the given text."""
    width = 70
    print()
    print(char * width)
    print(f" {text}")
    print(char * width)
    print()


def print_step(step: int, total: int, description: str):
    """Print a step indicator."""
    print(f"\n[Step {step}/{total}] {description}")
    print("-" * 50)


def check_aws_credentials() -> bool:
    """Verify AWS credentials are configured."""
    print("Checking AWS credentials...")
    try:
        import boto3
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"  ✓ AWS Account: {identity['Account']}")
        print(f"  ✓ User ARN: {identity['Arn']}")
        return True
    except Exception as e:
        print(f"  ✗ AWS credentials not configured: {e}")
        print("\n  Run 'aws configure' to set up your credentials.")
        return False


def check_default_vpc() -> bool:
    """Verify default VPC exists."""
    print("Checking default VPC...")
    try:
        import boto3
        ec2 = boto3.client('ec2')
        response = ec2.describe_vpcs(
            Filters=[{"Name": "is-default", "Values": ["true"]}]
        )
        if response["Vpcs"]:
            vpc_id = response["Vpcs"][0]["VpcId"]
            print(f"  ✓ Default VPC: {vpc_id}")
            return True
        else:
            print("  ✗ No default VPC found")
            print("\n  Run 'aws ec2 create-default-vpc' to create one.")
            return False
    except Exception as e:
        print(f"  ✗ Error checking VPC: {e}")
        return False


def preflight_checks() -> bool:
    """Run all preflight checks."""
    print_banner("PREFLIGHT CHECKS")
    
    checks = [
        ("AWS Credentials", check_aws_credentials),
        ("Default VPC", check_default_vpc),
    ]
    
    all_passed = True
    for name, check_func in checks:
        if not check_func():
            all_passed = False
    
    if all_passed:
        print("\n✓ All preflight checks passed!")
    else:
        print("\n✗ Some checks failed. Please fix the issues above.")
    
    return all_passed


# =============================================================================
# PHASE 1: Database Cluster
# =============================================================================

def deploy_phase1() -> dict:
    """
    Deploy Phase 1: Database Cluster
    - Create 3x t3.micro EC2 instances
    - Install MySQL 8 on each
    - Import Sakila database
    - Run sysbench benchmarks
    """
    print_banner("PHASE 1: DATABASE CLUSTER", "=")
    
    from infrastructure.db_nodes import create_db_nodes, get_db_nodes_status, print_db_status
    from infrastructure.setup_db import setup_all_db_nodes
    
    results = {
        "phase": 1,
        "status": "pending",
        "instances": [],
        "setup_results": [],
        "start_time": datetime.now().isoformat(),
    }
    
    try:
        # Step 1: Create EC2 instances
        print_step(1, 3, "Creating EC2 instances (3x t3.micro)")
        instances = create_db_nodes()
        results["instances"] = instances
        print_db_status()
        
        # Step 2: Wait for instances to be ready
        print_step(2, 3, "Waiting for instances to initialize")
        print("  Waiting 90 seconds for cloud-init...")
        for i in range(9):
            time.sleep(10)
            print(f"  ... {(i+1)*10} seconds")
        
        # Refresh instance info
        instances = get_db_nodes_status()
        running = [i for i in instances if i["state"] == "running"]
        print(f"\n  ✓ {len(running)}/3 instances running")
        
        # Step 3: Setup MySQL + Sakila + Sysbench
        print_step(3, 3, "Setting up MySQL + Sakila + Sysbench")
        setup_results = setup_all_db_nodes(running)
        results["setup_results"] = setup_results
        
        # Check results
        success_count = sum(1 for r in setup_results if r.get("success"))
        
        if success_count == 3:
            results["status"] = "success"
            print("\n✓ Phase 1 completed successfully!")
        else:
            results["status"] = "partial"
            print(f"\n⚠ Phase 1 partially completed ({success_count}/3 nodes)")
        
        results["end_time"] = datetime.now().isoformat()
        
    except Exception as e:
        results["status"] = "failed"
        results["error"] = str(e)
        print(f"\n✗ Phase 1 failed: {e}")
        import traceback
        traceback.print_exc()
    
    return results


# =============================================================================
# PHASE 2: Proxy + Gatekeeper + Replication
# =============================================================================

def deploy_phase2() -> dict:
    """
    Deploy Phase 2: Proxy, Gatekeeper, and Replication
    - Configure MySQL replication (Manager -> Workers)
    - Deploy Proxy (t3.small)
    - Deploy Gatekeeper (t3.small)
    - Configure security groups
    """
    print_banner("PHASE 2: PROXY + GATEKEEPER + REPLICATION", "=")
    
    results = {
        "phase": 2,
        "status": "pending",
        "start_time": datetime.now().isoformat(),
    }
    
    # TODO: Implement Phase 2
    print("  [TODO] Phase 2 will be implemented next:")
    print("    - MySQL replication (Manager -> Workers)")
    print("    - Proxy instance (t3.small)")
    print("    - Gatekeeper instance (t3.small)")
    print("    - Security groups (strict access)")
    print("    - Proxy strategies (direct_hit, random, customized)")
    
    results["status"] = "not_implemented"
    results["end_time"] = datetime.now().isoformat()
    
    return results


# =============================================================================
# PHASE 3: Benchmarks
# =============================================================================

def run_phase3_benchmarks() -> dict:
    """
    Run Phase 3: Benchmarks
    - 1000 writes + 1000 reads per strategy
    - Strategies: direct_hit, random, customized
    - Save results to results/ directory
    """
    print_banner("PHASE 3: BENCHMARKS", "=")
    
    results = {
        "phase": 3,
        "status": "pending",
        "strategies": {},
        "start_time": datetime.now().isoformat(),
    }
    
    # TODO: Implement Phase 3
    print("  [TODO] Phase 3 will be implemented after Phase 2:")
    print("    - Benchmark: direct_hit strategy")
    print("    - Benchmark: random strategy")
    print("    - Benchmark: customized (ping-based) strategy")
    print("    - 1000 writes + 1000 reads per strategy")
    print("    - Results saved to results/")
    
    results["status"] = "not_implemented"
    results["end_time"] = datetime.now().isoformat()
    
    return results


# =============================================================================
# DESTROY ALL RESOURCES
# =============================================================================

def destroy_all() -> bool:
    """Destroy all project resources."""
    print_banner("DESTROYING ALL RESOURCES", "!")
    
    from infrastructure.db_nodes import destroy_db_nodes
    from infrastructure.security_groups import delete_security_groups
    from infrastructure.keypair import delete_key_pair
    
    try:
        # Step 1: Terminate instances
        print_step(1, 3, "Terminating EC2 instances")
        destroy_db_nodes()
        # TODO: Destroy Proxy and Gatekeeper
        
        # Step 2: Wait and delete security groups
        print_step(2, 3, "Deleting security groups")
        print("  Waiting 10 seconds for instances to release SGs...")
        time.sleep(10)
        delete_security_groups()
        
        # Step 3: Delete key pair
        print_step(3, 3, "Deleting key pair")
        delete_key_pair()
        
        print("\n✓ All resources destroyed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Destruction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# =============================================================================
# STATUS
# =============================================================================

def show_status():
    """Show current status of all resources."""
    print_banner("CURRENT STATUS")
    
    from infrastructure.db_nodes import get_db_nodes_status, print_db_status
    
    # DB Nodes
    print_db_status()
    
    # TODO: Show Proxy and Gatekeeper status
    print("\n[Phase 2 resources: Not yet deployed]")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def full_deploy():
    """Run full deployment (all phases)."""
    print_banner("LOG8415E - FULL DEPLOYMENT", "#")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Preflight checks
    if not preflight_checks():
        print("\n✗ Preflight checks failed. Aborting.")
        return False
    
    # Confirm
    print("\nThis will deploy:")
    print("  - 3x t3.micro (DB nodes)")
    print("  - 1x t3.small (Proxy)")
    print("  - 1x t3.small (Gatekeeper)")
    print("\nEstimated time: 15-20 minutes")
    print("Estimated cost: ~$0.05/hour while running")
    
    response = input("\nProceed? [y/N]: ").strip().lower()
    if response != 'y':
        print("Aborted.")
        return False
    
    all_results = {}
    
    # Phase 1
    all_results["phase1"] = deploy_phase1()
    if all_results["phase1"]["status"] == "failed":
        print("\n✗ Phase 1 failed. Stopping deployment.")
        return False
    
    # Phase 2
    all_results["phase2"] = deploy_phase2()
    
    # Phase 3 (benchmarks)
    all_results["phase3"] = run_phase3_benchmarks()
    
    # Save results
    results_path = Path("results") / f"deployment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    results_path.parent.mkdir(exist_ok=True)
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n✓ Results saved to: {results_path}")
    
    # Summary
    print_banner("DEPLOYMENT SUMMARY")
    print(f"Phase 1 (DB Cluster):  {all_results['phase1']['status']}")
    print(f"Phase 2 (Proxy/GK):    {all_results['phase2']['status']}")
    print(f"Phase 3 (Benchmarks):  {all_results['phase3']['status']}")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="LOG8415E Final Assignment - Deployment Automation"
    )
    parser.add_argument(
        "--phase", 
        type=int, 
        choices=[1, 2, 3],
        help="Deploy only a specific phase"
    )
    parser.add_argument(
        "--destroy", 
        action="store_true",
        help="Destroy all resources"
    )
    parser.add_argument(
        "--status", 
        action="store_true",
        help="Show current status"
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompts"
    )
    
    args = parser.parse_args()
    
    # Add project root to path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    if args.destroy:
        if not args.no_confirm:
            response = input("Are you sure you want to destroy all resources? [y/N]: ")
            if response.strip().lower() != 'y':
                print("Aborted.")
                return
        destroy_all()
        
    elif args.status:
        show_status()
        
    elif args.phase == 1:
        if preflight_checks():
            deploy_phase1()
            
    elif args.phase == 2:
        if preflight_checks():
            deploy_phase2()
            
    elif args.phase == 3:
        run_phase3_benchmarks()
        
    else:
        full_deploy()


if __name__ == "__main__":
    main()


