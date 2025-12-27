#!/usr/bin/env python3
"""
LOG8415E Final Assignment - Main Entry Point
Cloud Design Patterns: Gatekeeper + Trusted Host

Usage:
    python main.py deploy      # Deploy all infrastructure
    python main.py status      # Show status of all resources
    python main.py destroy     # Destroy all resources
    python main.py deploy-db   # Deploy only DB nodes (Phase 1)
    python main.py status-db   # Show DB nodes status
    python main.py destroy-db  # Destroy only DB nodes
"""
import sys
import click


@click.group()
def cli():
    """LOG8415E Final Assignment - Infrastructure Management"""
    pass


# =============================================================================
# Phase 1: DB Nodes Commands
# =============================================================================

@cli.command("deploy-db")
def deploy_db():
    """Deploy the 3 DB nodes (manager + 2 workers)."""
    click.echo("\n" + "=" * 70)
    click.echo("PHASE 1: Deploying DB Nodes (EC2 instances only)")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import create_db_nodes, print_db_status
    
    try:
        instances = create_db_nodes()
        click.echo("\n[SUCCESS] DB nodes deployed!")
        print_db_status()
        
        click.echo("\n[NEXT STEP] Run 'python main.py setup-db' to install MySQL + Sakila + sysbench")
        
    except Exception as e:
        click.echo(f"\n[ERROR] Failed to deploy DB nodes: {e}", err=True)
        sys.exit(1)


@cli.command("setup-db")
def setup_db():
    """Setup MySQL, Sakila, and sysbench on all DB nodes (via SSH)."""
    click.echo("\n" + "=" * 70)
    click.echo("PHASE 1: Setting up DB Nodes (MySQL + Sakila + Sysbench)")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import get_db_nodes_status
    from infrastructure.setup_db import setup_all_db_nodes
    
    try:
        # Get current instances
        instances = get_db_nodes_status()
        
        if not instances:
            click.echo("[ERROR] No DB instances found. Run 'python main.py deploy-db' first.")
            sys.exit(1)
        
        running = [i for i in instances if i["state"] == "running"]
        if len(running) < 3:
            click.echo(f"[WARN] Only {len(running)}/3 instances are running")
        
        # Setup each node
        results = setup_all_db_nodes(running)
        
        # Summary
        click.echo("\n" + "=" * 70)
        click.echo("SETUP SUMMARY")
        click.echo("=" * 70)
        
        success_count = sum(1 for r in results if r.get("success"))
        click.echo(f"\nSuccessful: {success_count}/{len(results)}")
        
        for r in results:
            status = "✓" if r.get("success") else "✗"
            click.echo(f"  {status} {r.get('role', 'unknown')}: {r.get('host', 'N/A')}")
            if not r.get("success"):
                click.echo(f"      Error: {r.get('error', 'Unknown error')}")
        
        if success_count == len(results):
            click.echo("\n[SUCCESS] All DB nodes are ready!")
        else:
            click.echo("\n[WARN] Some nodes failed setup")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"\n[ERROR] Setup failed: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


@cli.command("status-db")
def status_db():
    """Show status of DB nodes."""
    from infrastructure.db_nodes import print_db_status
    print_db_status()


@cli.command("destroy-db")
@click.confirmation_option(prompt="Are you sure you want to destroy all DB nodes?")
def destroy_db():
    """Destroy all DB nodes."""
    click.echo("\n" + "=" * 70)
    click.echo("Destroying DB Nodes")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import destroy_db_nodes
    
    try:
        destroy_db_nodes()
        click.echo("\n[SUCCESS] All DB nodes destroyed!")
    except Exception as e:
        click.echo(f"\n[ERROR] Failed to destroy DB nodes: {e}", err=True)
        sys.exit(1)


# =============================================================================
# Full Deployment Commands (will be completed in Phase 2 & 3)
# =============================================================================

@cli.command("deploy")
def deploy_all():
    """Deploy all infrastructure (DB + Proxy + Gatekeeper)."""
    click.echo("\n" + "=" * 70)
    click.echo("FULL DEPLOYMENT")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import create_db_nodes, print_db_status
    from infrastructure.keypair import create_key_pair
    from infrastructure.security_groups import (
        create_db_security_group,
        create_proxy_security_group,
        create_gatekeeper_security_group
    )
    
    try:
        # Phase 1: DB Nodes
        click.echo("[Phase 1] Deploying DB nodes...")
        instances = create_db_nodes()
        
        # Phase 2: Proxy & Gatekeeper (placeholder for now)
        click.echo("\n[Phase 2] Creating security groups for Proxy & Gatekeeper...")
        create_proxy_security_group()
        create_gatekeeper_security_group()
        
        click.echo("\n[TODO] Proxy and Gatekeeper instances will be deployed in Phase 2")
        
        print_db_status()
        
    except Exception as e:
        click.echo(f"\n[ERROR] Deployment failed: {e}", err=True)
        sys.exit(1)


@cli.command("status")
def status_all():
    """Show status of all resources."""
    from infrastructure.db_nodes import print_db_status
    
    click.echo("\n" + "=" * 70)
    click.echo("INFRASTRUCTURE STATUS")
    click.echo("=" * 70)
    
    print_db_status()
    
    # TODO: Add Proxy and Gatekeeper status in Phase 2
    click.echo("\n[TODO] Proxy and Gatekeeper status will be added in Phase 2")


@cli.command("destroy")
@click.confirmation_option(prompt="Are you sure you want to destroy ALL resources?")
def destroy_all():
    """Destroy all resources."""
    click.echo("\n" + "=" * 70)
    click.echo("DESTROYING ALL RESOURCES")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import destroy_db_nodes
    from infrastructure.security_groups import delete_security_groups
    from infrastructure.keypair import delete_key_pair
    
    try:
        # Destroy instances first
        click.echo("[1/3] Destroying instances...")
        destroy_db_nodes()
        # TODO: Destroy Proxy and Gatekeeper in Phase 2
        
        # Then security groups
        click.echo("\n[2/3] Destroying security groups...")
        import time
        time.sleep(5)  # Wait for instances to release SGs
        delete_security_groups()
        
        # Finally key pair
        click.echo("\n[3/3] Destroying key pair...")
        delete_key_pair()
        
        click.echo("\n[SUCCESS] All resources destroyed!")
        
    except Exception as e:
        click.echo(f"\n[ERROR] Destruction failed: {e}", err=True)
        sys.exit(1)


# =============================================================================
# Utility Commands
# =============================================================================

@cli.command("ssh")
@click.argument("role", type=click.Choice(["manager", "worker1", "worker2"]))
def ssh_to_node(role: str):
    """Print SSH command for a specific DB node."""
    from infrastructure.db_nodes import get_db_nodes_status
    from infrastructure.keypair import get_key_path
    
    instances = get_db_nodes_status()
    
    for inst in instances:
        if inst["role"] == role:
            if inst.get("public_ip"):
                key_path = get_key_path()
                click.echo(f"\nSSH command for {role}:")
                click.echo(f"  ssh -i {key_path} ubuntu@{inst['public_ip']}")
                click.echo(f"\nCheck setup progress:")
                click.echo(f"  ssh -i {key_path} ubuntu@{inst['public_ip']} 'tail -100 /var/log/user-data.log'")
                click.echo(f"\nCheck MySQL status:")
                click.echo(f"  ssh -i {key_path} ubuntu@{inst['public_ip']} 'sudo mysqladmin ping'")
            else:
                click.echo(f"\n[ERROR] No public IP for {role}")
            return
    
    click.echo(f"\n[ERROR] Node '{role}' not found")


@cli.command("deploy-db-full")
def deploy_db_full():
    """Deploy AND setup DB nodes in one command."""
    click.echo("\n" + "=" * 70)
    click.echo("PHASE 1: Full DB Deployment (EC2 + MySQL + Sakila + Sysbench)")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import create_db_nodes, get_db_nodes_status, print_db_status
    from infrastructure.setup_db import setup_all_db_nodes
    import time
    
    try:
        # Step 1: Create instances
        click.echo("[Step 1/2] Creating EC2 instances...")
        instances = create_db_nodes()
        print_db_status()
        
        # Wait a bit for instances to be fully ready
        click.echo("\n[WAIT] Waiting 60 seconds for instances to initialize...")
        time.sleep(60)
        
        # Refresh instance info
        instances = get_db_nodes_status()
        running = [i for i in instances if i["state"] == "running"]
        
        # Step 2: Setup MySQL
        click.echo("\n[Step 2/2] Setting up MySQL + Sakila + Sysbench...")
        results = setup_all_db_nodes(running)
        
        # Summary
        success_count = sum(1 for r in results if r.get("success"))
        
        click.echo("\n" + "=" * 70)
        click.echo("DEPLOYMENT COMPLETE")
        click.echo("=" * 70)
        click.echo(f"\nSuccessful: {success_count}/{len(results)} nodes")
        
        if success_count == len(results):
            click.echo("\n[SUCCESS] Phase 1 completed! All DB nodes are ready.")
            click.echo("\n[NEXT] You can now proceed to Phase 2 (replication + proxy + gatekeeper)")
        else:
            click.echo("\n[WARN] Some nodes failed setup. Check logs above.")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"\n[ERROR] Deployment failed: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


@cli.command("verify-db")
def verify_db():
    """Verify that DB nodes are properly set up (via SSH)."""
    click.echo("\n" + "=" * 70)
    click.echo("VERIFYING DB NODES SETUP")
    click.echo("=" * 70 + "\n")
    
    from infrastructure.db_nodes import get_db_nodes_status
    from infrastructure.setup_db import verify_setup
    from infrastructure.ssh_client import SSHClient
    from infrastructure.config import get_config
    
    instances = get_db_nodes_status()
    config = get_config()
    
    if not instances:
        click.echo("[ERROR] No DB instances found")
        return
    
    for inst in instances:
        click.echo(f"\n--- {inst['role'].upper()} ({inst.get('public_ip', 'N/A')}) ---")
        
        if not inst.get("public_ip"):
            click.echo("  [SKIP] No public IP")
            continue
        
        if inst.get("state") != "running":
            click.echo(f"  [SKIP] Instance not running (state: {inst.get('state')})")
            continue
        
        try:
            with SSHClient(inst["public_ip"]) as ssh:
                results = verify_setup(ssh, config)
                
                mysql_ok = "✓" if results.get("mysql_running") else "✗"
                sakila_count = results.get("sakila_tables", 0)
                sakila_ok = "✓" if sakila_count >= 16 else "✗"
                sysbench_ok = "✓" if results.get("sysbench_completed") else "✗"
                
                click.echo(f"  {mysql_ok} MySQL running: {results.get('mysql_running')}")
                click.echo(f"  {sakila_ok} Sakila tables: {sakila_count}")
                click.echo(f"  {sysbench_ok} Sysbench completed: {results.get('sysbench_completed')}")
                
        except Exception as e:
            click.echo(f"  [ERROR] Cannot connect: {e}")


if __name__ == "__main__":
    cli()

