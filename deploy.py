#!/usr/bin/env python3
"""
Deployment Script - LOG8415E Final Assignment

Orchestrates the full deployment of the MySQL cluster with Gatekeeper pattern.

Usage:
    python deploy.py demo      # Full deployment
    python deploy.py destroy   # Cleanup all resources
    python deploy.py status    # Show current status
    python deploy.py benchmark # Run benchmarks only
"""
import sys
import time
import json
from datetime import datetime
from pathlib import Path


# =============================================================================
# Utility Functions
# =============================================================================

def print_banner(text: str, char: str = "="):
    """Print a formatted banner for section headers."""
    print(f"\n{char * 70}\n {text}\n{char * 70}\n")


def print_step(step: int, total: int, description: str):
    """Print a step indicator with progress (e.g., [Step 1/3])."""
    print(f"\n[Step {step}/{total}] {description}\n" + "-" * 50)


def preflight_checks() -> bool:
    """
    Verify AWS credentials and default VPC exist before deployment.
    Returns True if all checks pass, False otherwise.
    """
    print_banner("PREFLIGHT CHECKS")
    
    import boto3
    
    print("Checking AWS credentials...")
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"  ✓ AWS Account: {identity['Account']}")
    except Exception as e:
        print(f"  ✗ AWS credentials not configured: {e}")
        return False
    
    print("Checking default VPC...")
    try:
        ec2 = boto3.client('ec2')
        response = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
        if response["Vpcs"]:
            print(f"  ✓ Default VPC: {response['Vpcs'][0]['VpcId']}")
        else:
            print("  ✗ No default VPC found")
            return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False
    
    print("\n✓ All preflight checks passed!")
    return True


# =============================================================================
# Phase 1: Database Cluster
# =============================================================================

def deploy_phase1() -> dict:
    """
    Deploy 3 MySQL nodes (manager + 2 workers) on t3.micro instances.
    Installs MySQL, imports Sakila database, and runs sysbench benchmark.
    """
    print_banner("PHASE 1: DATABASE CLUSTER")
    
    from infrastructure.db_nodes import create_db_nodes, get_db_nodes_status, print_db_status
    from infrastructure.setup_db import setup_all_db_nodes
    
    results = {"phase": 1, "status": "pending"}
    
    try:
        print_step(1, 3, "Creating 3x t3.micro EC2 instances")
        create_db_nodes()
        print_db_status()
        
        print_step(2, 3, "Waiting for instances (4 min)")
        for i in range(8):
            time.sleep(30)
            print(f"  ... {(i+1)*30}s")
        
        print_step(3, 3, "Setting up MySQL + Sakila + Sysbench")
        running = [i for i in get_db_nodes_status() if i["state"] == "running"]
        setup_results = setup_all_db_nodes(running)
        
        success = sum(1 for r in setup_results if r.get("success"))
        results["status"] = "success" if success == 3 else "partial"
        print(f"\n✓ Phase 1: {success}/3 nodes ready")
        
    except Exception as e:
        results["status"] = "failed"
        print(f"\n✗ Phase 1 failed: {e}")
        import traceback; traceback.print_exc()
    
    return results


# =============================================================================
# Phase 2: Proxy + Gatekeeper + Replication
# =============================================================================

def deploy_phase2() -> dict:
    """
    Configure MySQL replication and deploy Proxy/Gatekeeper on t3.small instances.
    Sets up strict security group rules for the Gatekeeper pattern.
    """
    print_banner("PHASE 2: PROXY + GATEKEEPER + REPLICATION")
    
    from infrastructure.db_nodes import get_db_nodes_status
    from infrastructure.cluster_nodes import create_phase2_instances
    from infrastructure.security_groups import update_existing_sgs_strict_rules
    from infrastructure.replication import setup_replication
    from infrastructure.setup_proxy_gatekeeper import setup_proxy_and_gatekeeper
    from infrastructure.config import get_config
    
    results = {"phase": 2, "status": "pending"}
    
    try:
        print_step(1, 5, "Getting DB nodes")
        db_nodes = [n for n in get_db_nodes_status() if n.get("state") == "running"]
        if not db_nodes:
            raise RuntimeError("No DB nodes. Run Phase 1 first.")
        print(f"  Found {len(db_nodes)} running DB nodes")
        
        print_step(2, 5, "Configuring MySQL replication")
        setup_replication(db_nodes)
        
        print_step(3, 5, "Updating security groups")
        update_existing_sgs_strict_rules()
        
        print_step(4, 5, "Creating Proxy + Gatekeeper instances")
        instances = create_phase2_instances()
        
        print("  Waiting 180s for instances...")
        for i in range(6):
            time.sleep(30)
            print(f"  ... {(i+1)*30}s")
        
        print_step(5, 5, "Deploying applications")
        setup_results = setup_proxy_and_gatekeeper(
            instances.get("proxy"), 
            instances.get("gatekeeper"), 
            db_nodes
        )
        
        proxy_ok = setup_results.get("proxy", {}).get("success", False)
        gk_ok = setup_results.get("gatekeeper", {}).get("success", False)
        
        if proxy_ok and gk_ok:
            results["status"] = "success"
            gk = instances.get("gatekeeper", {})
            config = get_config()
            print(f"\n✓ Phase 2 completed!")
            print(f"  Gatekeeper: http://{gk.get('public_ip')}:{config['gatekeeper']['port']}")
        else:
            results["status"] = "partial"
            print(f"\n⚠ Phase 2 partial: Proxy={'✓' if proxy_ok else '✗'} GK={'✓' if gk_ok else '✗'}")
        
    except Exception as e:
        results["status"] = "failed"
        print(f"\n✗ Phase 2 failed: {e}")
        import traceback; traceback.print_exc()
    
    return results


# =============================================================================
# Phase 3: Benchmarks
# =============================================================================

def deploy_phase3() -> dict:
    """
    Run benchmarks testing all 3 routing strategies (direct_hit, random, customized).
    Executes 1000 writes + 1000 reads per strategy and saves results to JSON.
    """
    print_banner("PHASE 3: BENCHMARKS")
    
    from infrastructure.cluster_nodes import get_proxy_gatekeeper_status
    from infrastructure.config import get_config
    from benchmark import run_all_benchmarks
    
    results = {"phase": 3, "status": "pending"}
    
    try:
        config = get_config()
        gk = get_proxy_gatekeeper_status().get("gatekeeper", {})
        
        if gk.get("state") != "running" or not gk.get("public_ip"):
            raise RuntimeError("Gatekeeper not running. Deploy Phase 2 first.")
        
        gatekeeper_url = f"http://{gk['public_ip']}:{config['gatekeeper']['port']}"
        print(f"  Gatekeeper: {gatekeeper_url}")
        print(f"  Strategies: direct_hit, random, customized")
        print(f"  Queries: {config['benchmark']['num_writes']} writes + {config['benchmark']['num_reads']} reads each")
        
        run_all_benchmarks(
            gatekeeper_url=gatekeeper_url,
            api_key=config["gatekeeper"]["api_key"],
            num_writes=config['benchmark']['num_writes'],
            num_reads=config['benchmark']['num_reads'],
            output_dir=config['benchmark']['results_dir']
        )
        
        results["status"] = "success"
        print("\n✓ Phase 3 completed!")
        
    except Exception as e:
        results["status"] = "failed"
        print(f"\n✗ Phase 3 failed: {e}")
        import traceback; traceback.print_exc()
    
    return results


# =============================================================================
# CLI Commands
# =============================================================================

def cmd_demo():
    """
    Execute full deployment: Phase 1 (DB) + Phase 2 (Proxy/GK) + Phase 3 (Benchmarks).
    Automatically destroys all resources after benchmarks complete.
    """
    print_banner("LOG8415E - FULL DEPLOYMENT", "#")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nDeploying: 3x t3.micro (DB) + 1x t3.small (Proxy) + 1x t3.small (Gatekeeper)")
    
    if not preflight_checks():
        return
    
    results = {}
    
    results["phase1"] = deploy_phase1()
    if results["phase1"]["status"] == "failed":
        print("\n✗ Stopping - Phase 1 failed")
        return
    
    results["phase2"] = deploy_phase2()
    results["phase3"] = deploy_phase3()
    
    # Save results before cleanup
    results_path = Path("results") / f"deployment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    results_path.parent.mkdir(exist_ok=True)
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print_banner("DEPLOYMENT SUMMARY")
    print(f"Phase 1 (DB Cluster):  {results['phase1']['status']}")
    print(f"Phase 2 (Proxy/GK):    {results['phase2']['status']}")
    print(f"Phase 3 (Benchmarks):  {results['phase3']['status']}")
    print(f"\nResults saved: {results_path}")
    
    # Auto-cleanup after benchmarks
    print("\n[AUTO] Cleaning up resources...")
    cmd_destroy()


def cmd_destroy():
    """
    Terminate all EC2 instances, delete security groups, and remove key pair.
    Waits 15s before deleting security groups to allow instance termination.
    """
    print_banner("DESTROYING ALL RESOURCES", "!")
    
    from infrastructure.db_nodes import destroy_db_nodes
    from infrastructure.cluster_nodes import destroy_proxy_gatekeeper
    from infrastructure.security_groups import delete_security_groups
    from infrastructure.keypair import delete_key_pair
    
    try:
        print_step(1, 3, "Terminating EC2 instances")
        destroy_db_nodes()
        destroy_proxy_gatekeeper()
        
        print_step(2, 3, "Deleting security groups (waiting 15s)")
        time.sleep(15)
        delete_security_groups()
        
        print_step(3, 3, "Deleting key pair")
        delete_key_pair()
        
        print("\n✓ All resources destroyed!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback; traceback.print_exc()


def cmd_status():
    """
    Display current infrastructure status: DB nodes, Proxy, Gatekeeper.
    Shows connection info and sample curl command if Gatekeeper is running.
    """
    print_banner("CURRENT STATUS")
    
    from infrastructure.db_nodes import print_db_status
    from infrastructure.cluster_nodes import print_phase2_status, get_proxy_gatekeeper_status
    from infrastructure.config import get_config
    
    print_db_status()
    print_phase2_status()
    
    config = get_config()
    gk = get_proxy_gatekeeper_status().get("gatekeeper", {})
    
    if gk.get("state") == "running" and gk.get("public_ip"):
        port = config["gatekeeper"]["port"]
        api_key = config["gatekeeper"]["api_key"]
        print(f"\n{'=' * 70}")
        print("QUICK ACCESS")
        print(f"{'=' * 70}")
        print(f"\n  Gatekeeper: http://{gk['public_ip']}:{port}")
        print(f"  API Key: {api_key}")
        print(f"\n  Test: curl -X POST http://{gk['public_ip']}:{port}/query \\")
        print(f'         -H "X-API-Key: {api_key}" -H "Content-Type: application/json" \\')
        print(f'         -d \'{{"query": "SELECT COUNT(*) FROM sakila.actor"}}\'\n')


def cmd_benchmark():
    """Run Phase 3 benchmarks only (requires deployed infrastructure)."""
    deploy_phase3()


def cmd_help():
    """Display usage information and available commands."""
    print(__doc__)
    print("Commands:")
    print("  demo      Full deployment (Phase 1 + 2 + 3)")
    print("  destroy   Destroy all AWS resources")
    print("  status    Show current infrastructure status")
    print("  benchmark Run benchmarks (requires Phase 2)\n")


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Parse CLI arguments and execute the corresponding command."""
    sys.path.insert(0, str(Path(__file__).parent))
    
    if len(sys.argv) < 2:
        cmd_help()
        return
    
    command = sys.argv[1].lower()
    
    commands = {
        "demo": cmd_demo,
        "destroy": cmd_destroy,
        "status": cmd_status,
        "benchmark": cmd_benchmark,
        "help": cmd_help,
        "--help": cmd_help,
        "-h": cmd_help,
    }
    
    if command in commands:
        commands[command]()
    else:
        print(f"Unknown command: {command}")
        cmd_help()


if __name__ == "__main__":
    main()
