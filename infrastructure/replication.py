"""
MySQL Replication - LOG8415E Final Assignment

Configures GTID-based replication between MySQL nodes:
- Manager (master): Accepts writes, replicates to workers
- Workers (replicas): Read-only, receive replicated data

Replication flow: Manager -> Worker1, Manager -> Worker2
"""
import base64
import time
from typing import Dict, List
from .ssh_client import SSHClient, wait_for_ssh
from .config import get_config

# MySQL configuration file path
MYSQL_CNF = "/etc/mysql/mysql.conf.d/mysqld.cnf"


# =============================================================================
# MySQL Command Execution
# =============================================================================

def run_mysql_command(ssh: SSHClient, root_pass: str, sql: str, description: str = "") -> tuple:
    """
    Execute a MySQL command safely using a temporary SQL file.
    
    Uses base64 encoding to avoid shell escaping issues with special
    characters in SQL queries (quotes, semicolons, etc.).
    
    Args:
        ssh: SSH client connected to the MySQL server
        root_pass: MySQL root password
        sql: SQL command to execute
        description: Optional description for error logging
    
    Returns:
        Tuple of (success: bool, output: str)
    """
    # Encode SQL to base64 to avoid shell escaping issues
    sql_b64 = base64.b64encode(sql.encode()).decode()
    
    # Write to temp file and execute
    ssh.run(f"echo {sql_b64} | base64 -d > /tmp/mysql_cmd.sql", sudo=True, check=False)
    exit_code, stdout, stderr = ssh.run(
        f"mysql -u root -p{root_pass} < /tmp/mysql_cmd.sql 2>&1",
        sudo=True, check=False
    )
    ssh.run("rm -f /tmp/mysql_cmd.sql", sudo=True, check=False)
    
    # Filter out password warning from output
    output = "\n".join(l for l in (stdout + stderr).split("\n") if "Using a password" not in l)
    success = (exit_code == 0)
    
    if description and not success:
        print(f"    [{description}] Error: {output.strip()}")
    
    return success, output


# =============================================================================
# MySQL Daemon Configuration
# =============================================================================

def _set_mysql_cnf_kv(ssh: SSHClient, key: str, value: str) -> None:
    """
    Set a key=value pair in MySQL configuration file.
    
    Replaces existing value if key exists, appends if not.
    Handles both commented and uncommented lines.
    
    Args:
        ssh: SSH client
        key: Configuration key (e.g., 'bind-address')
        value: Configuration value (e.g., '0.0.0.0')
    """
    cmd = (
        f"if grep -q '^{key}\\s*=' {MYSQL_CNF} 2>/dev/null; then "
        f"  sed -i 's|^{key}\\s*=.*|{key} = {value}|' {MYSQL_CNF}; "
        f"elif grep -q '^#*{key}\\s*=' {MYSQL_CNF} 2>/dev/null; then "
        f"  sed -i 's|^#*{key}\\s*=.*|{key} = {value}|' {MYSQL_CNF}; "
        f"else "
        f"  echo '{key} = {value}' >> {MYSQL_CNF}; "
        f"fi"
    )
    ssh.run(cmd, sudo=True, check=False)


def ensure_mysql_network_and_id(ssh: SSHClient, server_id: int, is_manager: bool) -> bool:
    """
    Configure MySQL daemon for replication.
    
    Sets critical configuration options:
    - bind-address = 0.0.0.0 (listen on all interfaces, required for remote connections)
    - server-id (unique per node in the cluster)
    - Manager: log_bin, binlog_format, gtid_mode
    - Workers: read_only, super_read_only, relay_log
    
    Restarts MySQL and verifies it's listening on the network.
    
    Args:
        ssh: SSH client
        server_id: Unique server ID (1 for manager, 2/3 for workers)
        is_manager: True if this is the manager node
    
    Returns:
        True if MySQL is properly configured and listening
    """
    print(f"    Configuring MySQL daemon (server_id={server_id}, manager={is_manager})...")
    
    # Common settings
    _set_mysql_cnf_kv(ssh, "bind-address", "0.0.0.0")
    _set_mysql_cnf_kv(ssh, "server-id", str(server_id))
    
    if is_manager:
        # Manager needs binary logging for replication
        _set_mysql_cnf_kv(ssh, "log_bin", "/var/log/mysql/mysql-bin.log")
        _set_mysql_cnf_kv(ssh, "binlog_format", "ROW")
        _set_mysql_cnf_kv(ssh, "gtid_mode", "ON")
        _set_mysql_cnf_kv(ssh, "enforce_gtid_consistency", "ON")
    else:
        # Workers are read-only replicas
        _set_mysql_cnf_kv(ssh, "read_only", "ON")
        _set_mysql_cnf_kv(ssh, "super_read_only", "ON")
        _set_mysql_cnf_kv(ssh, "relay_log", "/var/log/mysql/mysql-relay-bin.log")
        _set_mysql_cnf_kv(ssh, "gtid_mode", "ON")
        _set_mysql_cnf_kv(ssh, "enforce_gtid_consistency", "ON")
    
    # Create log directory and restart MySQL
    ssh.run("mkdir -p /var/log/mysql && chown mysql:mysql /var/log/mysql", sudo=True, check=False)
    print("    Restarting MySQL...")
    ssh.run("systemctl restart mysql", sudo=True, check=False)
    time.sleep(3)
    
    # Verify MySQL is listening on network (not just localhost)
    exit_code, stdout, stderr = ssh.run("ss -lntp | grep 3306 || true", sudo=True, check=False)
    listen_output = stdout + stderr
    
    if "127.0.0.1:3306" in listen_output and "0.0.0.0:3306" not in listen_output:
        print("    ✗ MySQL listening only on localhost (bind-address issue)")
        return False
    
    if ":3306" not in listen_output:
        print("    ✗ MySQL not listening on port 3306")
        return False
    
    print(f"    ✓ MySQL listening on network")
    return True


# =============================================================================
# Manager Configuration
# =============================================================================

def configure_manager_for_replication(ssh: SSHClient, config: dict) -> bool:
    """
    Configure the manager node as replication source (master).
    
    Steps:
    1. Configure MySQL daemon (bind-address, server-id, GTID)
    2. Test MySQL connection
    3. Create replication user with REPLICATION SLAVE privilege
    4. Verify GTID mode is enabled
    
    Args:
        ssh: SSH client connected to manager
        config: Project configuration
    
    Returns:
        True if manager is properly configured
    """
    print("  [1/3] Configuring Manager for replication...")
    
    mysql_config = config["mysql"]
    root_pass = mysql_config["root_password"]
    repl_user = mysql_config["replication_user"]
    repl_pass = mysql_config["replication_password"]
    
    # Configure MySQL daemon
    if not ensure_mysql_network_and_id(ssh, server_id=1, is_manager=True):
        return False
    
    # Test MySQL connection
    print("    Testing MySQL connection...")
    success, _ = run_mysql_command(ssh, root_pass, "SELECT 1;", "Test")
    if not success:
        return False
    print("    ✓ MySQL connection OK")
    
    # Create replication user
    print("    Creating replication user...")
    run_mysql_command(ssh, root_pass, f"DROP USER IF EXISTS '{repl_user}'@'%';", "")
    
    create_sql = f"CREATE USER '{repl_user}'@'%' IDENTIFIED WITH mysql_native_password BY '{repl_pass}';"
    success, output = run_mysql_command(ssh, root_pass, create_sql, "Create user")
    
    if not success and "already exists" not in output.lower():
        create_sql = f"CREATE USER '{repl_user}'@'%' IDENTIFIED BY '{repl_pass}';"
        run_mysql_command(ssh, root_pass, create_sql, "Create user (alt)")
    
    run_mysql_command(ssh, root_pass, f"GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'%';", "Grant")
    run_mysql_command(ssh, root_pass, "FLUSH PRIVILEGES;", "Flush")
    
    # Verify user exists
    verify_sql = f"SELECT COUNT(*) FROM mysql.user WHERE user='{repl_user}';"
    success, output = run_mysql_command(ssh, root_pass, verify_sql, "Verify")
    
    user_exists = any(line.strip().isdigit() and int(line.strip()) > 0 
                      for line in output.strip().split('\n'))
    
    if not user_exists:
        print(f"    ✗ Replication user NOT FOUND")
        return False
    print(f"    ✓ Replication user: {repl_user}@%")
    
    # Verify GTID mode
    success, output = run_mysql_command(ssh, root_pass, "SHOW VARIABLES LIKE 'gtid_mode';", "GTID")
    print(f"    GTID Mode: {'✓ ON' if 'ON' in output else '✗ OFF'}")
    
    print("  [OK] Manager configured for replication")
    return True


# =============================================================================
# Worker Configuration
# =============================================================================

def configure_worker_as_replica(ssh: SSHClient, config: dict, manager_private_ip: str, server_id: int) -> bool:
    """
    Configure a worker node as a replica of the manager.
    
    Steps:
    1. Configure MySQL daemon (bind-address, server-id, read_only)
    2. Test network connectivity to manager
    3. Stop any existing replication
    4. Configure replication source (CHANGE REPLICATION SOURCE TO)
    5. Start replication
    
    Args:
        ssh: SSH client connected to worker
        config: Project configuration
        manager_private_ip: Private IP of the manager node
        server_id: Unique server ID (2 or 3)
    
    Returns:
        True if replica is properly configured
    """
    print(f"  [2/3] Configuring replica (server_id={server_id})...")
    print(f"    Manager IP: {manager_private_ip}")
    
    mysql_config = config["mysql"]
    root_pass = mysql_config["root_password"]
    repl_user = mysql_config["replication_user"]
    repl_pass = mysql_config["replication_password"]
    
    # Configure MySQL daemon
    if not ensure_mysql_network_and_id(ssh, server_id=server_id, is_manager=False):
        return False
    
    # Test network connectivity to manager
    print("    Testing connection to manager...")
    exit_code, stdout, stderr = ssh.run(f"nc -z -w 5 {manager_private_ip} 3306", sudo=True, check=False)
    if exit_code == 0:
        print(f"    ✓ Manager port 3306 reachable")
    else:
        print(f"    ✗ Cannot reach manager on port 3306")
    
    # Test MySQL authentication to manager
    test_conn = f"mysql -h {manager_private_ip} -u {repl_user} -p{repl_pass} -e 'SELECT 1' 2>&1"
    exit_code, stdout, stderr = ssh.run(test_conn, sudo=True, check=False)
    if exit_code == 0:
        print(f"    ✓ Authentication to manager successful")
    else:
        output_clean = "\n".join(l for l in (stdout + stderr).split("\n") if "Using a password" not in l)
        print(f"    ✗ Cannot authenticate to manager: {output_clean.strip()}")
    
    # Stop existing replication
    print("    Stopping existing replication...")
    run_mysql_command(ssh, root_pass, "STOP REPLICA;", "")
    run_mysql_command(ssh, root_pass, "RESET REPLICA ALL;", "")
    
    # Configure replication source with GTID auto-positioning
    print("    Configuring replication source...")
    change_source_sql = f"""CHANGE REPLICATION SOURCE TO
    SOURCE_HOST='{manager_private_ip}',
    SOURCE_USER='{repl_user}',
    SOURCE_PASSWORD='{repl_pass}',
    SOURCE_AUTO_POSITION=1,
    GET_SOURCE_PUBLIC_KEY=1;"""
    
    success, output = run_mysql_command(ssh, root_pass, change_source_sql, "Change Source")
    if not success:
        print(f"    ✗ Failed to configure source")
        return False
    
    # Start replication
    print("    Starting replication...")
    success, _ = run_mysql_command(ssh, root_pass, "START REPLICA;", "Start Replica")
    if not success:
        return False
    
    time.sleep(2)
    print("  [OK] Replica configured")
    return True


# =============================================================================
# Verification
# =============================================================================

def verify_replication_status(ssh: SSHClient, config: dict) -> Dict:
    """
    Verify replication status on a replica node.
    
    Parses SHOW REPLICA STATUS output to extract key metrics:
    - Replica_IO_Running: Connection to master is active
    - Replica_SQL_Running: SQL thread is applying changes
    - Seconds_Behind_Source: Replication lag
    
    Args:
        ssh: SSH client connected to replica
        config: Project configuration
    
    Returns:
        Dict with io_running, sql_running, seconds_behind, last_error
    """
    print("  [3/3] Verifying replication status...")
    
    root_pass = config["mysql"]["root_password"]
    success, stdout = run_mysql_command(ssh, root_pass, "SHOW REPLICA STATUS\\G", "")
    
    result = {
        "io_running": False,
        "sql_running": False,
        "seconds_behind": None,
        "last_error": None
    }
    
    for line in stdout.split('\n'):
        line = line.strip()
        if line.startswith("Replica_IO_Running:"):
            result["io_running"] = "Yes" in line
        elif line.startswith("Replica_SQL_Running:"):
            result["sql_running"] = "Yes" in line
        elif line.startswith("Seconds_Behind_Source:"):
            try:
                val = line.split(":")[-1].strip()
                result["seconds_behind"] = int(val) if val != "NULL" else None
            except:
                pass
        elif line.startswith("Last_IO_Error:") or line.startswith("Last_Error:"):
            error = line.split(":", 1)[-1].strip()
            if error:
                result["last_error"] = error
    
    print(f"    Replica_IO_Running: {'✓' if result['io_running'] else '✗'}")
    print(f"    Replica_SQL_Running: {'✓' if result['sql_running'] else '✗'}")
    print(f"    Seconds_Behind: {result['seconds_behind'] or 'N/A'}")
    
    if result["last_error"]:
        print(f"    Error: {result['last_error']}")
    
    return result


def test_replication(manager_ssh: SSHClient, worker_ssh: SSHClient, config: dict) -> bool:
    """
    Test replication by inserting data on manager and verifying on worker.
    
    Creates a test table, inserts a unique row on manager,
    then checks if the row appears on the worker (silently).
    
    Args:
        manager_ssh: SSH client connected to manager
        worker_ssh: SSH client connected to worker
        config: Project configuration
    
    Returns:
        True if test row was replicated to worker
    """
    root_pass = config["mysql"]["root_password"]
    test_value = f"replication_test_{int(time.time())}"
    
    # Create test table and insert row on manager
    create_sql = """CREATE TABLE IF NOT EXISTS sakila.replication_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        value VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    run_mysql_command(manager_ssh, root_pass, create_sql, "")
    run_mysql_command(manager_ssh, root_pass, 
                      f"INSERT INTO sakila.replication_test (value) VALUES ('{test_value}');", "")
    
    # Wait for replication
    time.sleep(3)
    
    # Check on worker
    check_sql = f"SELECT COUNT(*) FROM sakila.replication_test WHERE value='{test_value}';"
    success, stdout = run_mysql_command(worker_ssh, root_pass, check_sql, "")
    
    try:
        lines = [l.strip() for l in stdout.strip().split('\n') if l.strip()]
        return any(l.isdigit() and int(l) > 0 for l in lines)
    except:
        return False


# =============================================================================
# Main Setup Function
# =============================================================================

def setup_replication(db_nodes: List[Dict]) -> Dict:
    """
    Configure MySQL replication for the entire cluster.
    
    Orchestrates the full replication setup:
    1. Find manager and worker nodes from db_nodes list
    2. Configure manager as replication source
    3. Configure each worker as replica
    4. Verify replication status on each worker
    5. Run replication test
    
    Args:
        db_nodes: List of node dicts with 'role', 'public_ip', 'private_ip'
    
    Returns:
        Dict with 'success' bool and detailed results per node
    """
    config = get_config()
    
    # Find manager and workers
    manager = next((n for n in db_nodes if n.get("role") == "manager"), None)
    workers = [n for n in db_nodes if n.get("role") in ["worker1", "worker2"]]
    
    if not manager:
        return {"success": False, "error": "Manager not found"}
    if not workers:
        return {"success": False, "error": "No workers found"}
    
    print(f"\n{'='*60}")
    print("Configuring MySQL Replication")
    print(f"{'='*60}")
    print(f"  Manager: {manager.get('private_ip')}")
    for w in workers:
        print(f"  {w.get('role')}: {w.get('private_ip')}")
    
    results = {"manager": None, "workers": []}
    
    # Configure manager
    print(f"\n--- MANAGER ({manager.get('public_ip')}) ---")
    if not wait_for_ssh(manager.get("public_ip")):
        return {"success": False, "error": "Cannot SSH to manager"}
    
    with SSHClient(manager.get("public_ip")) as manager_ssh:
        manager_ok = configure_manager_for_replication(manager_ssh, config)
        results["manager"] = {"success": manager_ok}
        
        if not manager_ok:
            return {"success": False, "error": "Manager configuration failed"}
        
        # Configure each worker
        server_id = 2
        for worker in workers:
            print(f"\n--- {worker.get('role').upper()} ({worker.get('public_ip')}) ---")
            
            if not wait_for_ssh(worker.get("public_ip")):
                results["workers"].append({"role": worker.get("role"), "success": False})
                continue
            
            with SSHClient(worker.get("public_ip")) as worker_ssh:
                replica_ok = configure_worker_as_replica(
                    worker_ssh, config, manager.get("private_ip"), server_id
                )
                
                if replica_ok:
                    status = verify_replication_status(worker_ssh, config)
                    test_ok = test_replication(manager_ssh, worker_ssh, config)
                    
                    results["workers"].append({
                        "role": worker.get("role"),
                        "success": status["io_running"] and status["sql_running"],
                        "test_passed": test_ok
                    })
                else:
                    results["workers"].append({"role": worker.get("role"), "success": False})
                
                server_id += 1
    
    # Summary
    all_ok = results["manager"]["success"] and all(w.get("success") for w in results["workers"])
    results["success"] = all_ok
    
    print(f"\n{'='*60}")
    print("REPLICATION SUMMARY")
    print(f"{'='*60}")
    print(f"  Manager: {'✓' if results['manager']['success'] else '✗'}")
    for w in results["workers"]:
        print(f"  {w.get('role')}: {'✓' if w.get('success') else '✗'}")
    
    print(f"\n{'✓ Replication OK' if all_ok else '✗ Replication issues'}")
    return results


def verify_all_replication(db_nodes: List[Dict]) -> List[Dict]:
    """
    Verify replication status on all worker nodes.
    
    Used for checking replication health after setup.
    
    Args:
        db_nodes: List of node dicts with 'role', 'public_ip'
    
    Returns:
        List of status dicts for each worker
    """
    config = get_config()
    results = []
    
    for node in db_nodes:
        if node.get("role") not in ["worker1", "worker2"]:
            continue
        
        print(f"\n--- {node.get('role').upper()} ({node.get('public_ip')}) ---")
        
        if not node.get("public_ip"):
            results.append({"role": node.get("role"), "success": False, "error": "No public IP"})
            continue
        
        try:
            with SSHClient(node.get("public_ip")) as ssh:
                status = verify_replication_status(ssh, config)
                results.append({
                    "role": node.get("role"),
                    "success": status["io_running"] and status["sql_running"],
                    "status": status
                })
        except Exception as e:
            results.append({"role": node.get("role"), "success": False, "error": str(e)})
    
    return results
