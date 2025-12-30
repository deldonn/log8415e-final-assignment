"""
Database Setup - LOG8415E Final Assignment

Sets up MySQL 8 on EC2 instances:
- Installs and configures MySQL
- Creates application and replication users
- Imports Sakila sample database
- Runs sysbench OLTP benchmark

Each node gets a unique server_id for replication.
"""
from typing import Dict, List
from .ssh_client import SSHClient, wait_for_ssh
from .config import get_config


# =============================================================================
# MySQL Installation
# =============================================================================

def install_mysql(ssh: SSHClient, config: dict) -> bool:
    """
    Install MySQL 8 server on the remote host.
    
    Uses debconf to pre-configure root password for non-interactive install.
    Installs mysql-server and mysql-client packages.
    
    Args:
        ssh: SSH client connected to the host
        config: Project configuration with MySQL credentials
    
    Returns:
        True if installation successful
    """
    print("  [1/5] Installing MySQL 8...")
    
    root_password = config["mysql"]["root_password"]
    
    commands = f"""
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
debconf-set-selections <<< "mysql-server mysql-server/root_password password {root_password}"
debconf-set-selections <<< "mysql-server mysql-server/root_password_again password {root_password}"
apt-get install -y -qq mysql-server mysql-client
systemctl start mysql
systemctl enable mysql
echo "MySQL installed"
"""
    ssh.run(commands, sudo=True)
    return True


def configure_mysql(ssh: SSHClient, config: dict, server_id: int) -> bool:
    """
    Configure MySQL for replication and performance.
    
    Settings applied:
    - InnoDB buffer pool sized for t3.micro (256MB)
    - Binary logging enabled for replication
    - GTID mode enabled for auto-positioning
    - Bind address 0.0.0.0 for remote connections
    
    Args:
        ssh: SSH client
        config: Project configuration
        server_id: Unique server ID (1=manager, 2=worker1, 3=worker2)
    
    Returns:
        True if configuration successful
    """
    print("  [2/5] Configuring MySQL...")
    
    mysql_conf = f"""[mysqld]
# Performance settings for t3.micro (1GB RAM)
innodb_buffer_pool_size = 256M
innodb_log_file_size = 64M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT

# Network - allow remote connections
bind-address = 0.0.0.0

# Binary logging for replication
log_bin = /var/log/mysql/mysql-bin.log
server_id = {server_id}
binlog_format = ROW
binlog_expire_logs_seconds = 604800

# GTID replication
gtid_mode = ON
enforce_gtid_consistency = ON

# Connection limits
max_connections = 100
key_buffer_size = 16M
thread_cache_size = 8
"""
    
    commands = f"""
cat > /etc/mysql/mysql.conf.d/custom.cnf << 'MYSQLCONF'
{mysql_conf}
MYSQLCONF
systemctl restart mysql
sleep 3
echo "MySQL configured (server_id={server_id})"
"""
    ssh.run(commands, sudo=True)
    return True


# =============================================================================
# User Management
# =============================================================================

def create_mysql_users(ssh: SSHClient, config: dict) -> bool:
    """
    Create MySQL users for application and replication.
    
    Creates two users:
    - appuser: Full privileges for application queries
    - repl: REPLICATION SLAVE privilege for workers
    
    Both users use mysql_native_password for compatibility.
    
    Args:
        ssh: SSH client
        config: Project configuration with user credentials
    
    Returns:
        True if users created successfully
    """
    print("  [3/5] Creating MySQL users...")
    
    mysql = config["mysql"]
    root_pass = mysql["root_password"]
    app_user = mysql["app_user"]
    app_pass = mysql["app_password"]
    repl_user = mysql["replication_user"]
    repl_pass = mysql["replication_password"]
    
    # Set root password (fresh install uses socket auth)
    ssh.run(f"""
mysql -u root -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '{root_pass}';" 2>/dev/null || true
""", sudo=True, check=False)
    
    # Create application and replication users
    commands = f"""
mysql -u root -p'{root_pass}' -e "CREATE USER IF NOT EXISTS '{app_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{app_pass}';"
mysql -u root -p'{root_pass}' -e "GRANT ALL PRIVILEGES ON *.* TO '{app_user}'@'%' WITH GRANT OPTION;"
mysql -u root -p'{root_pass}' -e "CREATE USER IF NOT EXISTS '{repl_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{repl_pass}';"
mysql -u root -p'{root_pass}' -e "GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'%';"
mysql -u root -p'{root_pass}' -e "FLUSH PRIVILEGES;"
echo "Users created"
"""
    
    exit_code, _, stderr = ssh.run(commands, sudo=True, check=False)
    if exit_code != 0:
        print(f"    [WARN] User creation warnings: {stderr[:200]}")
    return True


# =============================================================================
# Sakila Database
# =============================================================================

def import_sakila(ssh: SSHClient, config: dict) -> bool:
    """
    Download and import the Sakila sample database.
    
    Sakila is a MySQL sample database with:
    - 200 actors, 1000 films, 600 customers
    - 23 tables with foreign keys and triggers
    
    Also creates the benchmark_results table for benchmarks
    (must be created here since Gatekeeper blocks DDL).
    
    Args:
        ssh: SSH client
        config: Project configuration
    
    Returns:
        True if import successful (200 actors found)
    """
    print("  [4/5] Importing Sakila database...")
    
    mysql = config["mysql"]
    root_pass = mysql["root_password"]
    app_user = mysql["app_user"]
    
    commands = f"""
set -e

# Skip if already imported
ACTOR_COUNT=$(mysql -u root -p'{root_pass}' -N -e "SELECT COUNT(*) FROM sakila.actor" 2>/dev/null || echo "0")
if [ "$ACTOR_COUNT" = "200" ]; then
    echo "Sakila already exists ($ACTOR_COUNT actors)"
    exit 0
fi

# Download Sakila
cd /tmp
rm -rf sakila-db sakila-db.tar.gz 2>/dev/null || true
wget -q --timeout=30 https://downloads.mysql.com/docs/sakila-db.tar.gz
tar -xzf sakila-db.tar.gz

# Import database
mysql -u root -p'{root_pass}' -e "DROP DATABASE IF EXISTS sakila; CREATE DATABASE sakila;"
mysql -u root -p'{root_pass}' sakila < sakila-db/sakila-schema.sql
mysql -u root -p'{root_pass}' sakila < sakila-db/sakila-data.sql
mysql -u root -p'{root_pass}' -e "GRANT ALL PRIVILEGES ON sakila.* TO '{app_user}'@'%'; FLUSH PRIVILEGES;"

# Create benchmark table (DDL blocked by Gatekeeper)
mysql -u root -p'{root_pass}' -e "
CREATE TABLE IF NOT EXISTS sakila.benchmark_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    strategy VARCHAR(50),
    query_type VARCHAR(10),
    latency_ms FLOAT,
    target_host VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
GRANT ALL PRIVILEGES ON sakila.benchmark_results TO '{app_user}'@'%';
FLUSH PRIVILEGES;
"

# Cleanup
rm -rf /tmp/sakila-db /tmp/sakila-db.tar.gz

# Verify
ACTOR_COUNT=$(mysql -u root -p'{root_pass}' -N -e "SELECT COUNT(*) FROM sakila.actor")
FILM_COUNT=$(mysql -u root -p'{root_pass}' -N -e "SELECT COUNT(*) FROM sakila.film")
echo "Sakila imported: $ACTOR_COUNT actors, $FILM_COUNT films"
"""
    
    exit_code, _, stderr = ssh.run(commands, sudo=True, check=False)
    if exit_code != 0:
        print(f"    [WARN] Sakila import issue: {stderr}")
    return exit_code == 0


# =============================================================================
# Sysbench Benchmark
# =============================================================================

def run_sysbench(ssh: SSHClient, config: dict) -> str:
    """
    Run sysbench OLTP benchmark on the MySQL instance.
    
    Benchmark configuration:
    - 2 tables with 5,000 rows each (reduced for speed)
    - 4 threads for concurrent queries
    - 15 seconds runtime (reduced from 60s)
    
    Args:
        ssh: SSH client
        config: Project configuration
    
    Returns:
        Benchmark output string
    """
    print("  [5/5] Running sysbench (15s)...")
    
    mysql = config["mysql"]
    app_user = mysql["app_user"]
    app_pass = mysql["app_password"]
    root_pass = mysql["root_password"]
    
    commands = f"""
apt-get install -y -qq sysbench
mysql -u root -p'{root_pass}' -e "CREATE DATABASE IF NOT EXISTS sbtest; GRANT ALL ON sbtest.* TO '{app_user}'@'%'; FLUSH PRIVILEGES;"

sysbench oltp_read_write --db-driver=mysql --mysql-host=127.0.0.1 --mysql-user='{app_user}' --mysql-password='{app_pass}' --mysql-db=sbtest --tables=2 --table-size=5000 prepare

sysbench oltp_read_write --db-driver=mysql --mysql-host=127.0.0.1 --mysql-user='{app_user}' --mysql-password='{app_pass}' --mysql-db=sbtest --tables=2 --table-size=5000 --threads=4 --time=15 run 2>&1 | tee /var/log/sysbench_results.txt

sysbench oltp_read_write --db-driver=mysql --mysql-host=127.0.0.1 --mysql-user='{app_user}' --mysql-password='{app_pass}' --mysql-db=sbtest --tables=2 cleanup

echo "Sysbench completed"
"""
    
    _, stdout, _ = ssh.run(commands, sudo=True)
    return stdout


# =============================================================================
# Verification
# =============================================================================

def verify_setup(ssh: SSHClient, config: dict) -> Dict[str, bool]:
    """
    Verify that database setup is complete.
    
    Checks:
    - MySQL service is running
    - Sakila database has 200 actors (expected count)
    - Sysbench results file exists
    
    Args:
        ssh: SSH client
        config: Project configuration
    
    Returns:
        Dict with verification results
    """
    print("  [VERIFY] Checking setup...")
    
    root_pass = config["mysql"]["root_password"]
    results = {}
    
    # Check MySQL is running
    _, stdout, _ = ssh.run(
        f"mysqladmin -u root -p'{root_pass}' ping 2>/dev/null || echo 'not alive'", 
        sudo=True, check=False
    )
    results["mysql_running"] = "alive" in stdout
    
    # Check Sakila database
    _, stdout, _ = ssh.run(
        f"mysql -u root -p'{root_pass}' -N -e \"SELECT COUNT(*) FROM sakila.actor\" 2>/dev/null || echo '0'",
        sudo=True, check=False
    )
    try:
        lines = [l.strip() for l in stdout.strip().split('\n') if l.strip()]
        actor_count = int(lines[-1]) if lines else 0
        results["sakila_actors"] = actor_count
        results["sakila_ok"] = (actor_count == 200)
    except (ValueError, IndexError):
        results["sakila_actors"] = 0
        results["sakila_ok"] = False
    
    # Check sysbench results
    exit_code, _, _ = ssh.run("test -f /var/log/sysbench_results.txt", sudo=True, check=False)
    results["sysbench_completed"] = (exit_code == 0)
    
    return results


# =============================================================================
# Main Setup Functions
# =============================================================================

def setup_db_node(host: str, role: str, server_id: int) -> Dict:
    """
    Complete setup for a single DB node.
    
    Runs all setup steps in order:
    1. Install MySQL
    2. Configure MySQL
    3. Create users
    4. Import Sakila
    5. Run sysbench
    6. Verify setup
    
    Args:
        host: Public IP of the EC2 instance
        role: Node role ('manager', 'worker1', 'worker2')
        server_id: MySQL server ID (1, 2, or 3)
    
    Returns:
        Dict with 'success' bool and setup details
    """
    config = get_config()
    
    print(f"\n{'='*60}")
    print(f"Setting up DB node: {role} ({host})")
    print(f"{'='*60}")
    
    if not wait_for_ssh(host):
        return {"success": False, "error": "SSH not available"}
    
    try:
        with SSHClient(host) as ssh:
            ssh.wait_for_cloud_init(timeout=300)
            
            install_mysql(ssh, config)
            configure_mysql(ssh, config, server_id)
            create_mysql_users(ssh, config)
            import_sakila(ssh, config)
            sysbench_output = run_sysbench(ssh, config)
            verification = verify_setup(ssh, config)
            
            print(f"\n  [RESULT] {role} setup complete")
            print(f"    MySQL: {'✓' if verification['mysql_running'] else '✗'}")
            print(f"    Sakila: {'✓' if verification.get('sakila_ok') else '✗'} ({verification.get('sakila_actors', 0)} actors)")
            print(f"    Sysbench: {'✓' if verification['sysbench_completed'] else '✗'}")
            
            return {
                "success": True,
                "role": role,
                "host": host,
                "verification": verification,
                "sysbench_output": sysbench_output[-2000:]
            }
            
    except Exception as e:
        print(f"  [ERROR] Setup failed: {e}")
        return {"success": False, "role": role, "host": host, "error": str(e)}


def setup_all_db_nodes(instances: List[Dict]) -> List[Dict]:
    """
    Setup all DB nodes sequentially.
    
    Assigns server IDs based on role:
    - manager: 1
    - worker1: 2
    - worker2: 3
    
    Args:
        instances: List of instance dicts with 'role' and 'public_ip'
    
    Returns:
        List of setup results for each node
    """
    results = []
    server_id_map = {"manager": 1, "worker1": 2, "worker2": 3}
    
    for inst in instances:
        if not inst.get("public_ip"):
            print(f"[SKIP] {inst['role']} - no public IP")
            results.append({"success": False, "role": inst["role"], "error": "No public IP"})
            continue
        
        server_id = server_id_map.get(inst["role"], 1)
        result = setup_db_node(inst["public_ip"], inst["role"], server_id)
        results.append(result)
    
    return results
