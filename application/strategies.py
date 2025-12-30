"""
Routing Strategies - LOG8415E Final Assignment

Defines routing strategies for distributing SQL queries across MySQL nodes.

Strategies:
- direct_hit: All queries go to manager (no distribution)
- random: READs go to random worker, WRITEs to manager
- customized: READs go to lowest-latency worker (ping-based)
"""
import random
import socket
import time
from typing import List

# =============================================================================
# Latency Measurement
# =============================================================================

def measure_tcp_latency(host: str, port: int = 3306, timeout: float = 2.0) -> float:
    """
    Measure TCP connection latency to a MySQL host.
    
    Creates a TCP socket connection and measures round-trip time.
    Used by customized strategy to find the fastest worker.
    
    Args:
        host: Target host IP address
        port: MySQL port (default 3306)
        timeout: Connection timeout in seconds
    
    Returns:
        Latency in milliseconds, or infinity if connection fails
    """
    try:
        start = time.perf_counter()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        latency = (time.perf_counter() - start) * 1000
        sock.close()
        return latency
    except (socket.timeout, socket.error, OSError):
        return float('inf')


# =============================================================================
# Base Strategy Class
# =============================================================================

class RoutingStrategy:
    """
    Abstract base class for routing strategies.
    
    All strategies must implement get_read_target().
    get_write_target() always returns manager (master).
    
    Attributes:
        manager_host: IP of the manager (master) node
        worker_hosts: List of worker (replica) node IPs
    """
    
    def __init__(self, manager_host: str, worker_hosts: List[str]):
        self.manager_host = manager_host
        self.worker_hosts = worker_hosts
    
    def get_read_target(self) -> str:
        """Get target host for READ queries. Override in subclasses."""
        raise NotImplementedError
    
    def get_write_target(self) -> str:
        """Get target host for WRITE queries. Always returns manager."""
        return self.manager_host


# =============================================================================
# Strategy Implementations
# =============================================================================

class DirectHitStrategy(RoutingStrategy):
    """
    Direct Hit Strategy.
    
    All queries (READ and WRITE) go to the manager node.
    Simplest strategy with no load distribution.
    Useful for testing or when consistency is critical.
    """
    
    def get_read_target(self) -> str:
        """Returns manager for all reads (no distribution)."""
        return self.manager_host


class RandomStrategy(RoutingStrategy):
    """
    Random Strategy.
    
    READ queries go to a randomly selected worker.
    WRITE queries go to the manager.
    Simple load balancing across workers.
    """
    
    def get_read_target(self) -> str:
        """Returns a random worker, or manager if no workers available."""
        if not self.worker_hosts:
            return self.manager_host
        return random.choice(self.worker_hosts)


class CustomizedPingStrategy(RoutingStrategy):
    """
    Customized (Ping-based) Strategy.
    
    READ queries go to the worker with lowest TCP latency.
    Uses caching to avoid measuring latency on every request.
    Falls back to random selection if all workers unreachable.
    
    Attributes:
        cache_ttl: How long to cache the best worker (seconds)
    """
    
    def __init__(self, manager_host: str, worker_hosts: List[str], cache_ttl: float = 5.0):
        super().__init__(manager_host, worker_hosts)
        self.cache_ttl = cache_ttl
        self._cached_best = None
        self._cache_time = 0
    
    def _measure_all_latencies(self) -> dict:
        """Measure TCP latency to all workers."""
        return {host: measure_tcp_latency(host) for host in self.worker_hosts}
    
    def get_read_target(self) -> str:
        """
        Returns the worker with lowest latency.
        Uses cached result if still valid (within TTL).
        """
        if not self.worker_hosts:
            return self.manager_host
        
        # Use cached result if still valid
        current_time = time.time()
        if self._cached_best and (current_time - self._cache_time) < self.cache_ttl:
            return self._cached_best
        
        # Measure latencies and find best worker
        latencies = self._measure_all_latencies()
        best_host = min(latencies, key=latencies.get, default=None)
        
        # Fall back to random if all unreachable
        if best_host is None or latencies.get(best_host) == float('inf'):
            best_host = random.choice(self.worker_hosts)
        
        # Cache result
        self._cached_best = best_host
        self._cache_time = current_time
        
        return best_host


# =============================================================================
# Factory Function
# =============================================================================

STRATEGIES = {
    "direct_hit": DirectHitStrategy,
    "random": RandomStrategy,
    "customized": CustomizedPingStrategy,
}


def get_strategy(name: str, manager_host: str, worker_hosts: List[str]) -> RoutingStrategy:
    """
    Factory function to create a strategy by name.
    
    Args:
        name: Strategy name ('direct_hit', 'random', 'customized')
        manager_host: IP of the manager node
        worker_hosts: List of worker node IPs
    
    Returns:
        RoutingStrategy instance
    
    Raises:
        ValueError: If strategy name is unknown
    """
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy: {name}. Available: {list(STRATEGIES.keys())}")
    
    return STRATEGIES[name](manager_host, worker_hosts)


# =============================================================================
# Query Classification
# =============================================================================

def classify_query(query: str) -> str:
    """
    Classify a SQL query as READ or WRITE.
    
    Used by the Proxy to determine routing:
    - READ queries can go to any node (based on strategy)
    - WRITE queries must go to manager
    
    Args:
        query: SQL query string
    
    Returns:
        'read' for SELECT queries (except FOR UPDATE)
        'write' for INSERT, UPDATE, DELETE, etc.
    """
    query_upper = query.strip().upper()
    
    # SELECT is read, except SELECT ... FOR UPDATE/SHARE (locking reads)
    if query_upper.startswith("SELECT"):
        if "FOR UPDATE" in query_upper or "FOR SHARE" in query_upper:
            return "write"
        return "read"
    
    return "write"
