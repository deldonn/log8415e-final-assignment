"""
Database setup module - Pure Python implementation.
Installs MySQL, imports Sakila, runs sysbench on remote EC2 instances.
"""
from typing import Dict, List
from .ssh_client import SSHClient, wait_for_ssh
from .config import get_config


def install_mysql(ssh: SSHClient, config: dict) -> bool:
    """Install and configure MySQL 8 on the remote host."""
    print("  [1/5] Installing MySQL 8...")
    
    mysql_config = config["mysql"]
    root_password = mysql_config["root_password"]
    
    # Update system and install MySQL
    commands = f"""
export DEBIAN_FRONTEND=noninteractive

# Update packages
apt-get update -qq

# Pre-configure MySQL root password
debconf-set-selections <<< "mysql-server mysql-server/root_password password {root_password}"
debconf-set-selections <<< "mysql-server mysql-server/root_password_again password {root_password}"

# Install MySQL
apt-get install -y -qq mysql-server mysql-client

# Start and enable MySQL
systemctl start mysql
systemctl enable mysql

echo "MySQL installed successfully"
"""
    
    ssh.run(commands, sudo=True)
    return True


def configure_mysql(ssh: SSHClient, config: dict, server_id: int) -> bool:
    """Configure MySQL for replication and performance."""
    print("  [2/5] Configuring MySQL...")
    
    # Create custom MySQL config
    mysql_conf = f"""[mysqld]
# Basic settings for t3.micro (1GB RAM)
innodb_buffer_pool_size = 256M
innodb_log_file_size = 64M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT

# Allow connections from any host
bind-address = 0.0.0.0

# Binary logging for replication
log_bin = /var/log/mysql/mysql-bin.log
server_id = {server_id}
binlog_format = ROW
binlog_expire_logs_seconds = 604800

# GTID replication
gtid_mode = ON
enforce_gtid_consistency = ON

# Performance
max_connections = 100
key_buffer_size = 16M
thread_cache_size = 8
"""
    
    commands = f"""
# Write MySQL config
cat > /etc/mysql/mysql.conf.d/custom.cnf << 'MYSQLCONF'
{mysql_conf}
MYSQLCONF

# Restart MySQL to apply config
systemctl restart mysql
sleep 3

echo "MySQL configured with server_id={server_id}"
"""
    
    ssh.run(commands, sudo=True)
    return True


def create_mysql_users(ssh: SSHClient, config: dict) -> bool:
    """Create MySQL users for app and replication."""
    print("  [3/5] Creating MySQL users...")
    
    mysql_config = config["mysql"]
    root_pass = mysql_config["root_password"]
    app_user = mysql_config["app_user"]
    app_pass = mysql_config["app_password"]
    repl_user = mysql_config["replication_user"]
    repl_pass = mysql_config["replication_password"]
    
    sql_commands = f"""
-- Update root password and allow connections
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '{root_pass}';

-- Create application user
CREATE USER IF NOT EXISTS '{app_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{app_pass}';

-- Create replication user
CREATE USER IF NOT EXISTS '{repl_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{repl_pass}';
GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'%';

FLUSH PRIVILEGES;
"""
    
    commands = f"""
mysql -u root << 'EOSQL'
{sql_commands}
EOSQL
echo "MySQL users created"
"""
    
    ssh.run(commands, sudo=True)
    return True


def import_sakila(ssh: SSHClient, config: dict) -> bool:
    """Download and import Sakila sample database."""
    print("  [4/5] Importing Sakila database...")
    
    mysql_config = config["mysql"]
    root_pass = mysql_config["root_password"]
    app_user = mysql_config["app_user"]
    
    commands = f"""
# Check if Sakila already exists
if mysql -u root -p'{root_pass}' -e "USE sakila" 2>/dev/null; then
    echo "Sakila database already exists"
    exit 0
fi

# Download Sakila
cd /tmp
wget -q https://downloads.mysql.com/docs/sakila-db.tar.gz
tar -xzf sakila-db.tar.gz

# Import schema and data
mysql -u root -p'{root_pass}' < sakila-db/sakila-schema.sql
mysql -u root -p'{root_pass}' < sakila-db/sakila-data.sql

# Grant app user access
mysql -u root -p'{root_pass}' -e "GRANT ALL PRIVILEGES ON sakila.* TO '{app_user}'@'%'; FLUSH PRIVILEGES;"

# Cleanup
rm -rf /tmp/sakila-db /tmp/sakila-db.tar.gz

# Verify
TABLE_COUNT=$(mysql -u root -p'{root_pass}' -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'sakila'")
echo "Sakila imported successfully with $TABLE_COUNT tables"
"""
    
    ssh.run(commands, sudo=True)
    return True


def run_sysbench(ssh: SSHClient, config: dict) -> str:
    """Install and run sysbench OLTP benchmark."""
    print("  [5/5] Running sysbench benchmark...")
    
    mysql_config = config["mysql"]
    app_user = mysql_config["app_user"]
    app_pass = mysql_config["app_password"]
    root_pass = mysql_config["root_password"]
    
    commands = f"""
# Install sysbench
apt-get install -y -qq sysbench

# Create sbtest database
mysql -u root -p'{root_pass}' -e "CREATE DATABASE IF NOT EXISTS sbtest; GRANT ALL ON sbtest.* TO '{app_user}'@'%'; FLUSH PRIVILEGES;"

# Prepare sysbench tables
sysbench oltp_read_write \\
    --db-driver=mysql \\
    --mysql-host=127.0.0.1 \\
    --mysql-user='{app_user}' \\
    --mysql-password='{app_pass}' \\
    --mysql-db=sbtest \\
    --tables=4 \\
    --table-size=10000 \\
    prepare

# Run benchmark (60 seconds)
sysbench oltp_read_write \\
    --db-driver=mysql \\
    --mysql-host=127.0.0.1 \\
    --mysql-user='{app_user}' \\
    --mysql-password='{app_pass}' \\
    --mysql-db=sbtest \\
    --tables=4 \\
    --table-size=10000 \\
    --threads=4 \\
    --time=60 \\
    --report-interval=10 \\
    run 2>&1 | tee /var/log/sysbench_results.txt

# Cleanup sysbench tables
sysbench oltp_read_write \\
    --db-driver=mysql \\
    --mysql-host=127.0.0.1 \\
    --mysql-user='{app_user}' \\
    --mysql-password='{app_pass}' \\
    --mysql-db=sbtest \\
    --tables=4 \\
    cleanup

echo "Sysbench completed"
"""
    
    exit_code, stdout, stderr = ssh.run(commands, sudo=True)
    return stdout


def verify_setup(ssh: SSHClient, config: dict) -> Dict[str, bool]:
    """Verify that the database setup is complete."""
    print("  [VERIFY] Checking setup...")
    
    mysql_config = config["mysql"]
    root_pass = mysql_config["root_password"]
    
    results = {}
    
    # Check MySQL is running (with password)
    exit_code, stdout, _ = ssh.run(
        f"mysqladmin -u root -p'{root_pass}' ping 2>/dev/null || echo 'not alive'", 
        sudo=True, check=False
    )
    results["mysql_running"] = "alive" in stdout
    
    # Check Sakila exists
    exit_code, stdout, _ = ssh.run(
        f"mysql -u root -p'{root_pass}' -N -e \"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'sakila'\" 2>/dev/null || echo '0'",
        sudo=True, check=False
    )
    try:
        table_count = int(stdout.strip())
        results["sakila_tables"] = table_count
    except ValueError:
        results["sakila_tables"] = 0
    
    # Check sysbench results exist
    exit_code, _, _ = ssh.run("test -f /var/log/sysbench_results.txt", sudo=True, check=False)
    results["sysbench_completed"] = exit_code == 0
    
    return results


def setup_db_node(host: str, role: str, server_id: int) -> Dict:
    """
    Complete setup for a single DB node.
    
    Args:
        host: Public IP of the instance
        role: 'manager', 'worker1', or 'worker2'
        server_id: Unique MySQL server ID (1, 2, or 3)
    
    Returns:
        Dictionary with setup results
    """
    config = get_config()
    
    print(f"\n{'='*60}")
    print(f"Setting up DB node: {role} ({host})")
    print(f"{'='*60}")
    
    # Wait for SSH
    if not wait_for_ssh(host):
        return {"success": False, "error": "SSH not available"}
    
    try:
        with SSHClient(host) as ssh:
            # Wait for cloud-init if running
            ssh.wait_for_cloud_init(timeout=300)
            
            # Run setup steps
            install_mysql(ssh, config)
            configure_mysql(ssh, config, server_id)
            create_mysql_users(ssh, config)
            import_sakila(ssh, config)
            sysbench_output = run_sysbench(ssh, config)
            
            # Verify
            verification = verify_setup(ssh, config)
            
            print(f"\n  [RESULT] Setup completed for {role}")
            print(f"    MySQL running: {verification['mysql_running']}")
            print(f"    Sakila tables: {verification['sakila_tables']}")
            print(f"    Sysbench done: {verification['sysbench_completed']}")
            
            return {
                "success": True,
                "role": role,
                "host": host,
                "verification": verification,
                "sysbench_output": sysbench_output[-2000:]  # Last 2000 chars
            }
            
    except Exception as e:
        print(f"  [ERROR] Setup failed for {role}: {e}")
        return {"success": False, "role": role, "host": host, "error": str(e)}


def setup_all_db_nodes(instances: List[Dict]) -> List[Dict]:
    """
    Setup all DB nodes sequentially.
    
    Args:
        instances: List of instance dicts with 'role', 'public_ip', etc.
    
    Returns:
        List of setup results
    """
    results = []
    server_id_map = {"manager": 1, "worker1": 2, "worker2": 3}
    
    for inst in instances:
        if not inst.get("public_ip"):
            print(f"[SKIP] {inst['role']} has no public IP")
            results.append({"success": False, "role": inst["role"], "error": "No public IP"})
            continue
        
        server_id = server_id_map.get(inst["role"], 1)
        result = setup_db_node(inst["public_ip"], inst["role"], server_id)
        results.append(result)
    
    return results


