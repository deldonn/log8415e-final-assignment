"""
Proxy & Gatekeeper Setup

Deploys the Proxy and Gatekeeper services on EC2 instances:
- Installs Python dependencies
- Deploys application code from application/ directory
- Creates systemd services for auto-start
- Verifies services are running

Deployment order: Proxy first (Gatekeeper depends on Proxy IP).
"""
import time
from pathlib import Path
from typing import Dict
from .ssh_client import SSHClient, wait_for_ssh
from .config import get_config


# =============================================================================
# Application Code Loading
# =============================================================================

def get_proxy_code() -> str:
    """Load the Proxy application code from local file."""
    app_path = Path(__file__).parent.parent / "application" / "proxy.py"
    return app_path.read_text(encoding="utf-8") if app_path.exists() else ""


def get_strategies_code() -> str:
    """Load the routing strategies module code from local file."""
    app_path = Path(__file__).parent.parent / "application" / "strategies.py"
    return app_path.read_text(encoding="utf-8") if app_path.exists() else ""


def get_gatekeeper_code() -> str:
    """Load the Gatekeeper application code from local file."""
    app_path = Path(__file__).parent.parent / "application" / "gatekeeper.py"
    return app_path.read_text(encoding="utf-8") if app_path.exists() else ""


# =============================================================================
# Systemd Service Templates
# =============================================================================

def get_proxy_service(config: dict) -> str:
    """Generate systemd service file for Proxy."""
    port = config["proxy"]["port"]
    return f"""[Unit]
Description=LOG8415E Proxy Service
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/app
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/ubuntu/app/venv/bin/uvicorn proxy:app --host 0.0.0.0 --port {port}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


def get_gatekeeper_service(config: dict) -> str:
    """Generate systemd service file for Gatekeeper."""
    port = config["gatekeeper"]["port"]
    return f"""[Unit]
Description=LOG8415E Gatekeeper Service
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/app
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/ubuntu/app/venv/bin/uvicorn gatekeeper:app --host 0.0.0.0 --port {port}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


# =============================================================================
# Setup Steps
# =============================================================================

def setup_python_env(ssh: SSHClient):
    """Setup Python virtual environment and install dependencies."""
    print("  [1/4] Setting up Python environment...")
    
    commands = """
mkdir -p /home/ubuntu/app
cd /home/ubuntu/app
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install fastapi uvicorn[standard] pymysql httpx
"""
    ssh.run(commands, sudo=False, check=True)
    print("  [OK] Python environment ready")


def deploy_proxy_app(ssh: SSHClient, config: dict, db_nodes: list):
    """Deploy Proxy application code to the instance."""
    print("  [2/4] Deploying Proxy application...")
    
    proxy_code = get_proxy_code()
    strategies_code = get_strategies_code()
    
    if not proxy_code or not strategies_code:
        raise RuntimeError("Proxy code not found in application/")
    
    # Extract DB node IPs
    manager_ip = None
    worker_ips = []
    for node in db_nodes:
        if node.get("role") == "manager":
            manager_ip = node.get("private_ip")
        elif node.get("role") in ["worker1", "worker2"]:
            worker_ips.append(node.get("private_ip"))
    
    # Generate config.py
    mysql = config["mysql"]
    config_code = f'''"""Auto-generated Proxy configuration."""
MYSQL_USER = "{mysql["app_user"]}"
MYSQL_PASSWORD = "{mysql["app_password"]}"
MYSQL_DATABASE = "{mysql["database"]}"
MANAGER_HOST = "{manager_ip}"
WORKER_HOSTS = {worker_ips}
PROXY_PORT = {config["proxy"]["port"]}
'''
    
    # Deploy files
    ssh.run(f"cat > /home/ubuntu/app/config.py << 'EOF'\n{config_code}\nEOF", sudo=False)
    ssh.run(f"cat > /home/ubuntu/app/strategies.py << 'EOF'\n{strategies_code}\nEOF", sudo=False)
    ssh.run(f"cat > /home/ubuntu/app/proxy.py << 'EOF'\n{proxy_code}\nEOF", sudo=False)
    
    print("  [OK] Proxy application deployed")


def deploy_gatekeeper_app(ssh: SSHClient, config: dict, proxy_private_ip: str):
    """Deploy Gatekeeper application code to the instance."""
    print("  [2/4] Deploying Gatekeeper application...")
    
    gatekeeper_code = get_gatekeeper_code()
    
    if not gatekeeper_code:
        raise RuntimeError("Gatekeeper code not found in application/")
    
    # Generate config.py
    config_code = f'''"""Auto-generated Gatekeeper configuration."""
API_KEY = "{config["gatekeeper"]["api_key"]}"
PROXY_HOST = "{proxy_private_ip}"
PROXY_PORT = {config["proxy"]["port"]}
GATEKEEPER_PORT = {config["gatekeeper"]["port"]}
'''
    
    # Deploy files
    ssh.run(f"cat > /home/ubuntu/app/config.py << 'EOF'\n{config_code}\nEOF", sudo=False)
    ssh.run(f"cat > /home/ubuntu/app/gatekeeper.py << 'EOF'\n{gatekeeper_code}\nEOF", sudo=False)
    
    print("  [OK] Gatekeeper application deployed")


def create_systemd_service(ssh: SSHClient, service_name: str, service_content: str):
    """Create and start a systemd service."""
    print(f"  [3/4] Creating systemd service: {service_name}...")
    
    ssh.run(f"cat > /etc/systemd/system/{service_name}.service << 'EOF'\n{service_content}\nEOF", sudo=True)
    ssh.run("systemctl daemon-reload", sudo=True)
    ssh.run(f"systemctl enable {service_name}", sudo=True)
    ssh.run(f"systemctl start {service_name}", sudo=True)
    
    print(f"  [OK] Service {service_name} started")


def verify_service(ssh: SSHClient, service_name: str, port: int) -> bool:
    """Verify a service is running and listening on its port."""
    print(f"  [4/4] Verifying {service_name}...")
    
    # Wait for uvicorn startup
    time.sleep(5)
    
    # Check systemd status
    _, stdout, _ = ssh.run(f"systemctl is-active {service_name}", sudo=True, check=False)
    service_active = "active" in stdout
    
    # Check port listening (with retry)
    port_listening = False
    for _ in range(3):
        _, stdout, _ = ssh.run(f"ss -tlnp | grep :{port}", sudo=True, check=False)
        if str(port) in stdout:
            port_listening = True
            break
        time.sleep(2)
    
    print(f"    Service: {'✓' if service_active else '✗'}")
    print(f"    Port {port}: {'✓' if port_listening else '✗'}")
    
    return service_active and port_listening


# =============================================================================
# Main Setup Functions
# =============================================================================

def setup_proxy(host: str, config: dict, db_nodes: list) -> Dict:
    """Complete setup for Proxy instance."""
    print(f"\n{'='*60}")
    print(f"Setting up Proxy ({host})")
    print(f"{'='*60}")
    
    if not wait_for_ssh(host):
        return {"success": False, "error": "SSH not available"}
    
    try:
        with SSHClient(host) as ssh:
            ssh.wait_for_cloud_init(timeout=300)
            
            setup_python_env(ssh)
            deploy_proxy_app(ssh, config, db_nodes)
            create_systemd_service(ssh, "proxy", get_proxy_service(config))
            success = verify_service(ssh, "proxy", config["proxy"]["port"])
            
            return {
                "success": success,
                "role": "proxy",
                "host": host,
                "port": config["proxy"]["port"]
            }
            
    except Exception as e:
        print(f"  [ERROR] Proxy setup failed: {e}")
        return {"success": False, "role": "proxy", "host": host, "error": str(e)}


def setup_gatekeeper(host: str, config: dict, proxy_private_ip: str) -> Dict:
    """Complete setup for Gatekeeper instance."""
    print(f"\n{'='*60}")
    print(f"Setting up Gatekeeper ({host})")
    print(f"{'='*60}")
    
    if not wait_for_ssh(host):
        return {"success": False, "error": "SSH not available"}
    
    try:
        with SSHClient(host) as ssh:
            ssh.wait_for_cloud_init(timeout=300)
            
            setup_python_env(ssh)
            deploy_gatekeeper_app(ssh, config, proxy_private_ip)
            create_systemd_service(ssh, "gatekeeper", get_gatekeeper_service(config))
            success = verify_service(ssh, "gatekeeper", config["gatekeeper"]["port"])
            
            return {
                "success": success,
                "role": "gatekeeper",
                "host": host,
                "port": config["gatekeeper"]["port"]
            }
            
    except Exception as e:
        print(f"  [ERROR] Gatekeeper setup failed: {e}")
        return {"success": False, "role": "gatekeeper", "host": host, "error": str(e)}


def setup_proxy_and_gatekeeper(proxy_info: dict, gatekeeper_info: dict, db_nodes: list) -> Dict:
    """Setup both Proxy and Gatekeeper instances. Proxy first (Gatekeeper needs its IP)."""
    config = get_config()
    results = {}
    
    # Setup Proxy first
    if proxy_info and proxy_info.get("public_ip"):
        results["proxy"] = setup_proxy(proxy_info["public_ip"], config, db_nodes)
    else:
        results["proxy"] = {"success": False, "error": "No Proxy instance"}
    
    # Setup Gatekeeper
    if gatekeeper_info and gatekeeper_info.get("public_ip"):
        proxy_private_ip = proxy_info.get("private_ip") if proxy_info else None
        if proxy_private_ip:
            results["gatekeeper"] = setup_gatekeeper(
                gatekeeper_info["public_ip"], config, proxy_private_ip
            )
        else:
            results["gatekeeper"] = {"success": False, "error": "No Proxy private IP"}
    else:
        results["gatekeeper"] = {"success": False, "error": "No Gatekeeper instance"}
    
    return results
