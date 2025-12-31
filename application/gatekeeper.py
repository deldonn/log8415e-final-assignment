"""
Gatekeeper Service 

Single public-facing entry point (Gatekeeper + Trusted Host patterns).
Responsibilities:
- Authentication via X-API-Key header
- SQL query validation (blocks dangerous operations)
- Routing to internal Proxy service
"""
import logging
import re
import time
from typing import Optional, List, Any

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx

# =============================================================================
# Configuration (auto-generated during deployment)
# =============================================================================

try:
    from config import API_KEY, PROXY_HOST, PROXY_PORT, GATEKEEPER_PORT
except ImportError:
    # Default values for local testing
    API_KEY = "log8415e-secret-key-2024"
    PROXY_HOST = "localhost"
    PROXY_PORT = 8000
    GATEKEEPER_PORT = 8080

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("gatekeeper")

# Proxy URL (internal, not publicly exposed)
PROXY_URL = f"http://{PROXY_HOST}:{PROXY_PORT}"

# =============================================================================
# Pydantic Models (Request/Response)
# =============================================================================

class QueryRequest(BaseModel):
    """SQL query request from client."""
    query: str
    args: Optional[List[Any]] = None
    strategy: Optional[str] = None  # Optional strategy (direct_hit, random, customized)


class QueryResponse(BaseModel):
    """SQL query response to client."""
    success: bool
    data: Optional[List[dict]] = None
    rows_affected: Optional[int] = None
    target_host: Optional[str] = None
    query_type: Optional[str] = None
    strategy: Optional[str] = None
    latency_ms: Optional[float] = None
    error: Optional[str] = None


# =============================================================================
# SQL Query Validation (Security)
# =============================================================================

# Blocked SQL patterns for security
BLOCKED_PATTERNS = [
    r'\bDROP\s+(TABLE|DATABASE|INDEX|VIEW|TRIGGER|PROCEDURE|FUNCTION)\b',
    r'\bALTER\s+(TABLE|DATABASE)\b', 
    r'\bCREATE\s+(TABLE|DATABASE|INDEX|VIEW|TRIGGER|PROCEDURE|FUNCTION)\b',
    r'\bTRUNCATE\s+TABLE\b',
    r'\bGRANT\b',
    r'\bREVOKE\b',
    r'\bLOAD_FILE\s*\(',
    r'\bINTO\s+(OUTFILE|DUMPFILE)\b',
    r'\bSHUTDOWN\b',
]
BLOCKED_REGEX = [re.compile(p, re.IGNORECASE) for p in BLOCKED_PATTERNS]
MAX_QUERY_LENGTH = 10000


def validate_query(query: str) -> tuple[bool, str]:
    """
    Validate a SQL query for security.
    
    Checks: max length, non-empty, no multi-statements, no DDL/dangerous operations.
    """
    if len(query) > MAX_QUERY_LENGTH:
        return False, f"Query too long (max {MAX_QUERY_LENGTH} characters)"
    
    if not query.strip():
        return False, "Empty query"
    
    # SQL injection prevention: no multi-statements
    if ';' in query.strip().rstrip(';'):
        return False, "Multiple statements not allowed"
    
    # Check for blocked patterns
    for pattern in BLOCKED_REGEX:
        if pattern.search(query):
            return False, f"Blocked operation detected: {pattern.pattern}"
    
    return True, ""


def verify_api_key(api_key: Optional[str]) -> bool:
    """Verify the API key provided in X-API-Key header."""
    return api_key == API_KEY if api_key else False


# =============================================================================
# Proxy Communication (Internal)
# =============================================================================

async def forward_to_proxy(endpoint: str, payload: dict) -> dict:
    """Forward a request to the Proxy service."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(f"{PROXY_URL}{endpoint}", json=payload)
            return response.json()
        except httpx.RequestError as e:
            logger.error(f"Proxy error: {e}")
            raise HTTPException(status_code=502, detail=f"Proxy unavailable: {e}")


async def change_proxy_strategy(strategy: str) -> bool:
    """Change the routing strategy on the Proxy."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(f"{PROXY_URL}/strategy", json={"strategy": strategy})
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Strategy change failed: {e}")
            return False


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="LOG8415E Gatekeeper",
    description="Public gateway with authentication and SQL validation",
    version="1.0.0"
)


@app.get("/health")
async def health_check():
    """Health check endpoint (no authentication required)."""
    return {"status": "healthy", "proxy_url": PROXY_URL}


@app.get("/")
async def root():
    """Root endpoint - Service information."""
    return {
        "service": "LOG8415E Gatekeeper",
        "endpoints": ["/health", "/query", "/query/direct", "/query/random", "/query/customized"]
    }


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest, x_api_key: Optional[str] = Header(None)):
    """
    Execute a SQL query through the Proxy.
    
    Process: Authentication -> Validation -> Strategy change -> Forward to Proxy.
    """
    # 1. Authentication
    if not verify_api_key(x_api_key):
        logger.warning("Unauthorized attempt")
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # 2. Validation
    is_valid, error_msg = validate_query(request.query)
    if not is_valid:
        logger.warning(f"Validation failed: {error_msg}")
        return QueryResponse(success=False, error=f"Query validation failed: {error_msg}")
    
    # 3. Change strategy if requested
    if request.strategy:
        await change_proxy_strategy(request.strategy)
    
    # 4. Forward to Proxy
    start_time = time.perf_counter()
    
    try:
        result = await forward_to_proxy("/query", {"query": request.query, "args": request.args})
        latency = (time.perf_counter() - start_time) * 1000
        
        return QueryResponse(
            success=result.get("success", False),
            data=result.get("data"),
            rows_affected=result.get("rows_affected"),
            target_host=result.get("target_host"),
            query_type=result.get("query_type"),
            strategy=result.get("strategy"),
            latency_ms=round(latency, 2),
            error=result.get("error")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Execution error: {e}")
        return QueryResponse(success=False, error=str(e))


@app.post("/query/direct")
async def direct_query(request: QueryRequest, x_api_key: Optional[str] = Header(None)):
    """Execute a query with direct_hit strategy. All queries go to the manager."""
    request.strategy = "direct_hit"
    return await execute_query(request, x_api_key)


@app.post("/query/random")
async def random_query(request: QueryRequest, x_api_key: Optional[str] = Header(None)):
    """Execute a query with random strategy. READs go to a random worker."""
    request.strategy = "random"
    return await execute_query(request, x_api_key)


@app.post("/query/customized")
async def customized_query(request: QueryRequest, x_api_key: Optional[str] = Header(None)):
    """Execute a query with customized (ping-based) strategy. READs go to lowest-latency worker."""
    request.strategy = "customized"
    return await execute_query(request, x_api_key)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler. Returns errors in standardized JSON format."""
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=GATEKEEPER_PORT)
