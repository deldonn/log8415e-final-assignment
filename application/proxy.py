"""
Proxy Service

Internal service that routes SQL queries to MySQL nodes.
Routing is based on:
- Query type: READ vs WRITE
- Strategy: direct_hit, random, or customized (ping-based)

WRITE queries always go to the manager (master).
READ queries are routed based on the active strategy.
"""
import logging
import time
from typing import Optional, Any, List
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pymysql

# =============================================================================
# Configuration (auto-generated during deployment)
# =============================================================================

try:
    from config import (
        MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
        MANAGER_HOST, WORKER_HOSTS, PROXY_PORT
    )
except ImportError:
    # Default values for local testing
    MYSQL_USER = "appuser"
    MYSQL_PASSWORD = "AppUser456!"
    MYSQL_DATABASE = "sakila"
    MANAGER_HOST = "localhost"
    WORKER_HOSTS = []
    PROXY_PORT = 8000

from strategies import get_strategy, classify_query, RoutingStrategy

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("proxy")

# =============================================================================
# Pydantic Models (Request/Response)
# =============================================================================

class QueryRequest(BaseModel):
    """SQL query request from Gatekeeper."""
    query: str
    args: Optional[List[Any]] = None


class QueryResponse(BaseModel):
    """SQL query response to Gatekeeper."""
    success: bool
    data: Optional[List[dict]] = None
    rows_affected: Optional[int] = None
    target_host: str
    query_type: str  # 'read' or 'write'
    strategy: str
    latency_ms: float
    error: Optional[str] = None


class StrategyRequest(BaseModel):
    """Strategy change request."""
    strategy: str


# =============================================================================
# Database Connection
# =============================================================================

@contextmanager
def get_db_connection(host: str):
    """
    Create a MySQL connection to the specified host.
    Uses context manager pattern for automatic connection cleanup.
    """
    conn = None
    try:
        conn = pymysql.connect(
            host=host,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30
        )
        yield conn
    finally:
        if conn:
            conn.close()


def execute_query(host: str, query: str, args: Optional[List] = None) -> dict:
    """
    Execute a SQL query on the specified MySQL host.
    SELECT returns rows, other statements return rows_affected.
    """
    with get_db_connection(host) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, args or ())
            
            if query.strip().upper().startswith("SELECT"):
                return {"data": cursor.fetchall()}
            else:
                conn.commit()
                return {"rows_affected": cursor.rowcount}


# =============================================================================
# Strategy Management
# =============================================================================

# Global strategy state
current_strategy: Optional[RoutingStrategy] = None
current_strategy_name: str = "direct_hit"


def init_strategy(name: str = "direct_hit"):
    """
    Initialize or change the routing strategy.
    Available: direct_hit, random, customized.
    """
    global current_strategy, current_strategy_name
    
    current_strategy = get_strategy(name, MANAGER_HOST, WORKER_HOSTS)
    current_strategy_name = name
    logger.info(f"Strategy set to: {name}")


def get_target_host(query_type: str) -> str:
    """
    Determine target MySQL host based on query type and strategy.
    WRITEs always go to manager, READs are routed based on strategy.
    """
    if current_strategy is None:
        init_strategy()
    
    if query_type == "write":
        return current_strategy.get_write_target()
    else:
        return current_strategy.get_read_target()


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="LOG8415E Proxy",
    description="SQL query router with multiple routing strategies",
    version="1.0.0"
)


@app.on_event("startup")
async def startup_event():
    """FastAPI startup event handler. Initializes the default routing strategy."""
    init_strategy(current_strategy_name)
    logger.info(f"Proxy started with strategy: {current_strategy_name}")
    logger.info(f"Manager: {MANAGER_HOST}")
    logger.info(f"Workers: {WORKER_HOSTS}")


@app.get("/health")
async def health_check():
    """Health check endpoint. Returns current configuration and strategy."""
    return {
        "status": "healthy",
        "strategy": current_strategy_name,
        "manager": MANAGER_HOST,
        "workers": WORKER_HOSTS
    }


@app.get("/strategy")
async def get_current_strategy():
    """Get current routing strategy."""
    return {
        "strategy": current_strategy_name,
        "available": ["direct_hit", "random", "customized"]
    }


@app.post("/strategy")
async def set_strategy(request: StrategyRequest):
    """Change the routing strategy."""
    try:
        init_strategy(request.strategy)
        return {"success": True, "strategy": current_strategy_name}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/query", response_model=QueryResponse)
async def execute_sql_query(request: QueryRequest):
    """
    Execute a SQL query with automatic routing.
    Process: Classify -> Get target -> Execute -> Return results.
    """
    start_time = time.perf_counter()
    
    # Classify query type (read/write)
    query_type = classify_query(request.query)
    
    # Get target host based on strategy
    target_host = get_target_host(query_type)
    
    logger.info(f"Query [{query_type}] -> {target_host} (strategy: {current_strategy_name})")
    
    try:
        result = execute_query(target_host, request.query, request.args)
        latency = (time.perf_counter() - start_time) * 1000
        
        return QueryResponse(
            success=True,
            data=result.get("data"),
            rows_affected=result.get("rows_affected"),
            target_host=target_host,
            query_type=query_type,
            strategy=current_strategy_name,
            latency_ms=round(latency, 2)
        )
        
    except pymysql.Error as e:
        latency = (time.perf_counter() - start_time) * 1000
        logger.error(f"MySQL error: {e}")
        
        return QueryResponse(
            success=False,
            target_host=target_host,
            query_type=query_type,
            strategy=current_strategy_name,
            latency_ms=round(latency, 2),
            error=str(e)
        )


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PROXY_PORT)
