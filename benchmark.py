#!/usr/bin/env python3
"""
Benchmark Script - LOG8415E Final Assignment

Tests the three Proxy routing strategies:
- direct_hit: All queries go to manager (baseline)
- random: READs distributed randomly across workers
- customized: READs go to lowest-latency worker

Uses parallel execution for faster benchmarking.
Results saved to JSON files in results/ directory.
"""
import argparse
import json
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import requests

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_NUM_WRITES = 1000
DEFAULT_NUM_READS = 1000
PARALLEL_WORKERS = 10  # Number of concurrent threads
STRATEGIES = ["direct_hit", "random", "customized"]

# Sample READ queries using Sakila database tables
READ_QUERIES = [
    "SELECT * FROM sakila.actor WHERE actor_id = {actor_id}",
    "SELECT COUNT(*) FROM sakila.film",
    "SELECT * FROM sakila.film WHERE film_id = {film_id}",
    "SELECT a.first_name, a.last_name, COUNT(fa.film_id) as film_count FROM sakila.actor a JOIN sakila.film_actor fa ON a.actor_id = fa.actor_id WHERE a.actor_id = {actor_id} GROUP BY a.actor_id",
    "SELECT * FROM sakila.customer WHERE customer_id = {customer_id}",
]


# =============================================================================
# Statistics Calculation
# =============================================================================

def calculate_stats(results: list, num_queries: int, elapsed_time: float) -> Dict:
    """
    Calculate benchmark statistics from query results.
    
    Args:
        results: List of query result dicts
        num_queries: Total queries attempted
        elapsed_time: Total wall-clock time in seconds
    
    Returns:
        Dict with computed statistics
    """
    errors = sum(1 for r in results if not r["success"])
    latencies = [r["latency_ms"] for r in results if r["success"]]
    targets = {}
    for r in results:
        host = r.get("target_host", "unknown")
        targets[host] = targets.get(host, 0) + 1
    
    if not latencies:
        return {
            "count": num_queries,
            "successful": 0,
            "errors": errors,
            "elapsed_time_s": elapsed_time,
            "avg_latency_ms": 0,
            "throughput_qps": 0,
            "targets": targets
        }
    
    sorted_latencies = sorted(latencies)
    return {
        "count": num_queries,
        "successful": len(latencies),
        "errors": errors,
        "elapsed_time_s": round(elapsed_time, 2),
        "avg_latency_ms": round(statistics.mean(latencies), 2),
        "min_latency_ms": round(min(latencies), 2),
        "max_latency_ms": round(max(latencies), 2),
        "p50_latency_ms": round(statistics.median(latencies), 2),
        "p95_latency_ms": round(sorted_latencies[int(len(latencies) * 0.95)], 2) if len(latencies) > 20 else round(max(latencies), 2),
        "p99_latency_ms": round(sorted_latencies[int(len(latencies) * 0.99)], 2) if len(latencies) > 100 else round(max(latencies), 2),
        "throughput_qps": round(len(latencies) / elapsed_time, 1) if elapsed_time > 0 else 0,
        "targets": targets
    }


# =============================================================================
# Query Execution
# =============================================================================

def ensure_benchmark_table(gatekeeper_url: str, api_key: str) -> bool:
    """
    Verify the benchmark_results table exists.
    
    Args:
        gatekeeper_url: Gatekeeper URL
        api_key: API key
    
    Returns:
        True if table exists
    """
    try:
        response = requests.post(
            f"{gatekeeper_url}/query/direct",
            json={"query": "SELECT COUNT(*) FROM sakila.benchmark_results"},
            headers={"X-API-Key": api_key},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and data.get("error"):
                print(f"[ERROR] Benchmark table not found: {data.get('error')}")
                return False
            print("[OK] Benchmark table ready")
            return True
        
        print(f"[ERROR] Cannot verify table: HTTP {response.status_code}")
        return False
    except Exception as e:
        print(f"[ERROR] Cannot connect to Gatekeeper: {e}")
        return False


def execute_query(gatekeeper_url: str, api_key: str, query: str, strategy: str) -> Dict:
    """
    Execute a single SQL query through the Gatekeeper.
    
    Args:
        gatekeeper_url: Gatekeeper URL
        api_key: API key
        query: SQL query string
        strategy: Routing strategy name
    
    Returns:
        Dict with success, latency_ms, target_host, error
    """
    start_time = time.perf_counter()
    
    endpoints = {
        "direct_hit": "/query/direct",
        "random": "/query/random",
        "customized": "/query/customized"
    }
    endpoint = endpoints.get(strategy, "/query")
    
    try:
        response = requests.post(
            f"{gatekeeper_url}{endpoint}",
            json={"query": query},
            headers={"X-API-Key": api_key},
            timeout=30
        )
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        
        if response.status_code == 200:
            data = response.json()
            success = not (isinstance(data, dict) and data.get("error"))
            return {
                "success": success,
                "latency_ms": latency_ms,
                "target_host": data.get("target_host", "unknown") if isinstance(data, dict) else "unknown",
                "error": data.get("error") if isinstance(data, dict) else None
            }
        
        return {"success": False, "latency_ms": latency_ms, "target_host": "unknown", "error": f"HTTP {response.status_code}"}
        
    except requests.RequestException as e:
        return {"success": False, "latency_ms": (time.perf_counter() - start_time) * 1000, "target_host": "unknown", "error": str(e)}


# =============================================================================
# Parallel Benchmark Runners
# =============================================================================

def run_write_benchmark(gatekeeper_url: str, api_key: str, strategy: str, num_queries: int = DEFAULT_NUM_WRITES) -> Dict:
    """
    Run WRITE benchmark with INSERT queries (parallel execution).
    
    Args:
        gatekeeper_url: Gatekeeper URL
        api_key: API key
        strategy: Current strategy name
        num_queries: Number of INSERT queries
    
    Returns:
        Dict with benchmark statistics
    """
    print(f"\n  Running {num_queries} WRITE queries...")
    
    def make_write_query(i):
        query = f"INSERT INTO sakila.benchmark_results (strategy, query_type, latency_ms, target_host, created_at) VALUES ('{strategy}', 'write', 0, 'pending', NOW())"
        return execute_query(gatekeeper_url, api_key, query, strategy)
    
    results = []
    first_error_shown = False
    start_time = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(make_write_query, i): i for i in range(num_queries)}
        completed = 0
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            
            if not result["success"] and not first_error_shown:
                print(f"    [DEBUG] First error: {result.get('error', 'unknown')}")
                first_error_shown = True
            
            if completed % 500 == 0:
                print(f"    ... {completed}/{num_queries}")
    
    elapsed = time.perf_counter() - start_time
    return calculate_stats(results, num_queries, elapsed)


def run_read_benchmark(gatekeeper_url: str, api_key: str, strategy: str, num_queries: int = DEFAULT_NUM_READS) -> Dict:
    """
    Run READ benchmark with SELECT queries (parallel execution).
    
    Args:
        gatekeeper_url: Gatekeeper URL
        api_key: API key
        strategy: Current strategy name
        num_queries: Number of SELECT queries
    
    Returns:
        Dict with benchmark statistics
    """
    print(f"\n  Running {num_queries} READ queries...")
    
    def make_read_query(i):
        query = random.choice(READ_QUERIES).format(
            actor_id=random.randint(1, 200),
            film_id=random.randint(1, 1000),
            customer_id=random.randint(1, 599)
        )
        return execute_query(gatekeeper_url, api_key, query, strategy)
    
    results = []
    start_time = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(make_read_query, i): i for i in range(num_queries)}
        completed = 0
        
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            
            if completed % 500 == 0:
                print(f"    ... {completed}/{num_queries}")
    
    elapsed = time.perf_counter() - start_time
    return calculate_stats(results, num_queries, elapsed)


def run_strategy_benchmark(gatekeeper_url: str, api_key: str, strategy: str, num_writes: int, num_reads: int) -> Dict:
    """
    Run complete benchmark for a single strategy.
    
    Args:
        gatekeeper_url: Gatekeeper URL
        api_key: API key
        strategy: Strategy to test
        num_writes: Number of write queries
        num_reads: Number of read queries
    
    Returns:
        Dict with strategy name, timestamp, and statistics
    """
    print(f"\n{'='*70}")
    print(f" BENCHMARK: {strategy.upper()}")
    print(f"{'='*70}")
    
    start_time = time.time()
    write_stats = run_write_benchmark(gatekeeper_url, api_key, strategy, num_writes)
    read_stats = run_read_benchmark(gatekeeper_url, api_key, strategy, num_reads)
    total_time = time.time() - start_time
    
    # Print results
    print(f"\n  WRITES: {write_stats['successful']}/{num_writes} ok, {write_stats.get('throughput_qps', 0):.0f} QPS, avg {write_stats.get('avg_latency_ms', 0):.0f}ms")
    print(f"  READS:  {read_stats['successful']}/{num_reads} ok, {read_stats.get('throughput_qps', 0):.0f} QPS, avg {read_stats.get('avg_latency_ms', 0):.0f}ms")
    print(f"  Targets: {read_stats.get('targets', {})}")
    print(f"  Total: {total_time:.1f}s")
    
    return {
        "strategy": strategy,
        "timestamp": datetime.now().isoformat(),
        "total_time_s": round(total_time, 2),
        "writes": write_stats,
        "reads": read_stats
    }


def run_all_benchmarks(gatekeeper_url: str, api_key: str, num_writes: int, num_reads: int, output_dir: str) -> Dict:
    """
    Run benchmarks for all three strategies and save results.
    
    Args:
        gatekeeper_url: Gatekeeper URL
        api_key: API key
        num_writes: Writes per strategy
        num_reads: Reads per strategy
        output_dir: Directory for result files
    
    Returns:
        Dict with all benchmark results
    """
    print("\n" + "=" * 70)
    print(" LOG8415E - BENCHMARK SUITE (Parallel)")
    print("=" * 70)
    print(f"\nGatekeeper: {gatekeeper_url}")
    print(f"Config: {num_writes} writes + {num_reads} reads per strategy")
    
    print("\n[Setup] Verifying benchmark table...")
    ensure_benchmark_table(gatekeeper_url, api_key)
    
    all_results = {
        "timestamp": datetime.now().isoformat(),
        "gatekeeper_url": gatekeeper_url,
        "num_writes": num_writes,
        "num_reads": num_reads,
        "parallel_workers": PARALLEL_WORKERS,
        "strategies": {}
    }
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    for strategy in STRATEGIES:
        result = run_strategy_benchmark(gatekeeper_url, api_key, strategy, num_writes, num_reads)
        all_results["strategies"][strategy] = result
        
        filename = f"benchmark_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_path / filename, "w") as f:
            json.dump(result, f, indent=2)
    
    # Save combined results
    combined_file = f"benchmark_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path / combined_file, "w") as f:
        json.dump(all_results, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 70)
    print(" SUMMARY")
    print("=" * 70)
    print(f"\n{'Strategy':<15} {'Write QPS':<12} {'Read QPS':<12} {'Avg Latency':<12} {'Errors':<10}")
    print("-" * 61)
    
    for strategy, data in all_results["strategies"].items():
        w = data.get("writes", {})
        r = data.get("reads", {})
        avg_lat = (w.get("avg_latency_ms", 0) + r.get("avg_latency_ms", 0)) / 2
        errors = w.get("errors", 0) + r.get("errors", 0)
        print(f"{strategy:<15} {w.get('throughput_qps', 0):<12.0f} {r.get('throughput_qps', 0):<12.0f} {avg_lat:<12.0f} {errors:<10}")
    
    print(f"\nResults saved to: {output_dir}/")
    return all_results


# =============================================================================
# Auto-detection
# =============================================================================

def get_gatekeeper_url_from_status() -> Optional[str]:
    """Auto-detect Gatekeeper URL from deployed infrastructure."""
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        
        from infrastructure.cluster_nodes import get_proxy_gatekeeper_status
        from infrastructure.config import get_config
        
        config = get_config()
        status = get_proxy_gatekeeper_status()
        
        gk = status.get("gatekeeper")
        if gk and gk.get("public_ip") and gk.get("state") == "running":
            return f"http://{gk['public_ip']}:{config['gatekeeper']['port']}"
        
        return None
    except Exception as e:
        print(f"[WARN] Auto-detect failed: {e}")
        return None


def get_api_key_from_config() -> str:
    """Get API key from project configuration."""
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from infrastructure.config import get_config
        return get_config()["gatekeeper"]["api_key"]
    except:
        return "log8415e-secret-key-2024"


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="LOG8415E Benchmark - Test routing strategies")
    parser.add_argument("--gatekeeper", "-g", type=str, help="Gatekeeper URL")
    parser.add_argument("--api-key", "-k", type=str, help="API key")
    parser.add_argument("--strategy", "-s", choices=STRATEGIES, help="Single strategy to test")
    parser.add_argument("--all", "-a", action="store_true", help="Test all strategies")
    parser.add_argument("--auto", action="store_true", help="Auto-detect Gatekeeper")
    parser.add_argument("--writes", "-w", type=int, default=DEFAULT_NUM_WRITES, help="Write queries per strategy")
    parser.add_argument("--reads", "-r", type=int, default=DEFAULT_NUM_READS, help="Read queries per strategy")
    parser.add_argument("--output", "-o", type=str, default="results", help="Output directory")
    
    args = parser.parse_args()
    
    # Get Gatekeeper URL
    gatekeeper_url = args.gatekeeper
    if args.auto or not gatekeeper_url:
        detected = get_gatekeeper_url_from_status()
        if detected:
            gatekeeper_url = detected
            print(f"[Auto] Gatekeeper: {gatekeeper_url}")
        elif not gatekeeper_url:
            print("[ERROR] No Gatekeeper found. Use --gatekeeper or --auto")
            return 1
    
    api_key = args.api_key or get_api_key_from_config()
    
    # Run benchmarks
    if args.all or not args.strategy:
        run_all_benchmarks(gatekeeper_url, api_key, args.writes, args.reads, args.output)
    else:
        result = run_strategy_benchmark(gatekeeper_url, api_key, args.strategy, args.writes, args.reads)
        
        output_path = Path(args.output)
        output_path.mkdir(exist_ok=True)
        filename = f"benchmark_{args.strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_path / filename, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved: {output_path / filename}")
    
    return 0


if __name__ == "__main__":
    exit(main())
