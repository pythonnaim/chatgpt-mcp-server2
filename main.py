from fastapi import FastAPI, Request, HTTPException, Header, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import psycopg
import os
import json
import asyncio
from dotenv import load_dotenv
from typing import Dict, Optional, List, Any, Union
from contextlib import contextmanager
import logging
from datetime import datetime
import time
import threading
from queue import Queue, Empty

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()  # Load DB creds from .env

app = FastAPI(
    title="Multi-Database MCP Server",
    description="Advanced multi-database connection and query service",
    version="3.0.0"
)

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer(auto_error=False)
AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN")

# Dynamically load database configurations from environment variables
def load_database_configs():
    """
    Dynamically load database configurations from .env file
    Looks for POSTGRES_URL_* environment variables where * becomes the database name
    """
    configs = {}
    
    # Get all environment variables
    env_vars = os.environ
    
    # Find all POSTGRES_URL_* variables
    for key, value in env_vars.items():
        if key.startswith("POSTGRES_URL_") and value:
            # Extract database name from environment variable
            # POSTGRES_URL_PRIMARY -> primary (convert to lowercase)
            db_name = key.replace("POSTGRES_URL_", "").lower()
            
            # Default configuration values
            default_pool_size = "10" if db_name == "primary" else "5"
            default_timeout = "60" if db_name in ["reporting", "reports"] else "30"
            
            # Build configuration for this database
            configs[db_name] = {
                "url": value,
                "pool_size": int(os.getenv(f"POSTGRES_POOL_SIZE_{db_name.upper()}", default_pool_size)),
                "max_overflow": int(os.getenv(f"POSTGRES_MAX_OVERFLOW_{db_name.upper()}", "20")),
                "timeout": int(os.getenv(f"POSTGRES_TIMEOUT_{db_name.upper()}", default_timeout)),
                "description": db_name
            }

    
    return configs

# Load database configurations dynamically
DATABASE_CONFIGS = load_database_configs()

# Connection pools storage - simple implementation
connection_pools: Dict[str, Queue] = {}
pool_locks: Dict[str, threading.Lock] = {}

class SimpleConnectionPool:
    """Simple connection pool implementation"""
    
    def __init__(self, database_url: str, pool_size: int = 10, timeout: int = 30):
        self.database_url = database_url
        self.pool_size = pool_size
        self.timeout = timeout
        self.pool = Queue(maxsize=pool_size)
        self.lock = threading.Lock()
        self.total_connections = 0
        
        # Pre-fill the pool
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        for _ in range(min(3, self.pool_size)):  # Start with 3 connections
            try:
                conn = psycopg.connect(self.database_url, autocommit=True)
                self.pool.put(conn)
                self.total_connections += 1
            except Exception as e:
                logger.error(f"Failed to create initial connection: {e}")
                break
    
    def get_connection(self):
        """Get a connection from the pool"""
        try:
            # Try to get an existing connection
            conn = self.pool.get(block=False)
            
            # Test if connection is still alive
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return conn
            except Exception:
                # Connection is dead, create a new one
                conn.close()
                self.total_connections -= 1
        except Empty:
            pass
        
        # Create new connection if pool is empty or connection was dead
        with self.lock:
            if self.total_connections < self.pool_size:
                try:
                    conn = psycopg.connect(self.database_url, autocommit=True)
                    self.total_connections += 1
                    return conn
                except Exception as e:
                    logger.error(f"Failed to create new connection: {e}")
                    raise
        
        # Wait for a connection to become available
        try:
            conn = self.pool.get(timeout=self.timeout)
            # Test the connection
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except Empty:
            raise Exception("Connection pool timeout")
        except Exception:
            # Connection is dead, try one more time
            conn.close()
            self.total_connections -= 1
            raise Exception("Failed to get valid connection from pool")
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        try:
            if not conn.closed:
                self.pool.put(conn, block=False)
            else:
                self.total_connections -= 1
        except Exception:
            # Pool is full or connection is bad
            try:
                conn.close()
            except:
                pass
            self.total_connections -= 1
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get(block=False)
                conn.close()
            except:
                pass
        self.total_connections = 0
    
    def get_stats(self):
        """Get pool statistics"""
        return {
            "total_connections": self.total_connections,
            "available_connections": self.pool.qsize(),
            "max_connections": self.pool_size
        }

class JsonRpcRequest(BaseModel):
    jsonrpc: str = Field(default="2.0")
    method: str
    id: Union[int, str]
    params: dict = Field(default_factory=dict)

class DatabaseInfo(BaseModel):
    name: str
    description: str
    connected: bool
    pool_size: int
    max_overflow: int
    timeout: int
    active_connections: Optional[int] = None
    total_connections: Optional[int] = None

def verify_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> bool:
    """Verify authentication token"""
    if not AUTH_TOKEN:
        return True  # Skip auth if no token configured
    
    if not credentials:
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    if credentials.credentials != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    
    return True

async def initialize_connection_pools():
    """Initialize connection pools for all configured databases"""
    global connection_pools
    
    for db_name, config in DATABASE_CONFIGS.items():
        try:
            logger.info(f"Initializing connection pool for {db_name}...")
            
            pool = SimpleConnectionPool(
                config["url"],
                pool_size=config["pool_size"],
                timeout=config["timeout"]
            )
            
            # Test the pool with a simple query
            conn = pool.get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                pool.return_connection(conn)
            
            connection_pools[db_name] = pool
            logger.info(f"✅ Connection pool initialized for {db_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize pool for {db_name}: {str(e)}")
            # Don't add to pools if initialization failed

async def close_connection_pools():
    """Close all connection pools"""
    global connection_pools
    
    for db_name, pool in connection_pools.items():
        try:
            pool.close_all()
            logger.info(f"Closed connection pool for {db_name}")
        except Exception as e:
            logger.error(f"Error closing pool for {db_name}: {str(e)}")
    
    connection_pools.clear()

@contextmanager
def get_sync_connection(database: str = "primary"):
    """Get a synchronous database connection with proper error handling and pooling"""
    if database not in DATABASE_CONFIGS:
        available = list(DATABASE_CONFIGS.keys())
        raise ValueError(f"Database '{database}' not found. Available: {available}")
    
    db_config = DATABASE_CONFIGS[database]
    if not db_config["url"]:
        raise ValueError(f"Database '{database}' URL is not configured")
    
    # Try to use connection pool first
    if database in connection_pools:
        pool = connection_pools[database]
        conn = None
        try:
            conn = pool.get_connection()
            yield conn
        except Exception as e:
            logger.error(f"Pool connection error for {database}: {str(e)}")
            raise
        finally:
            if conn:
                pool.return_connection(conn)
    else:
        # Fallback to direct connection
        try:
            with psycopg.connect(
                db_config["url"], 
                autocommit=True,
                connect_timeout=db_config["timeout"]
            ) as conn:
                yield conn
        except Exception as e:
            logger.error(f"Direct connection error for {database}: {str(e)}")
            raise

def get_pool_stats(database: str) -> dict:
    """Get connection pool statistics"""
    if database in connection_pools:
        return connection_pools[database].get_stats()
    return {"error": "No pool available for this database"}

def test_database_connection(database: str) -> tuple[bool, str, dict]:
    """Test database connection and return detailed info"""
    try:
        if database not in DATABASE_CONFIGS:
            available = list(DATABASE_CONFIGS.keys())
            return False, f"Database '{database}' not found. Available: {available}", {}
        
        config = DATABASE_CONFIGS[database]
        start_time = time.time()
        
        with get_sync_connection(database) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version(), current_database(), current_user, now()")
                result = cur.fetchone()
                
                # Get database stats
                cur.execute("""
                    SELECT 
                        pg_database_size(current_database()) as db_size,
                        (SELECT count(*) FROM information_schema.tables 
                         WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as table_count
                """)
                stats = cur.fetchone()
        
        response_time = round((time.time() - start_time) * 1000, 2)
        
        connection_info = {
            "version": result[0].split()[1] if result[0] else "Unknown",
            "database_name": result[1],
            "user": result[2],
            "server_time": result[3].isoformat() if result[3] else None,
            "database_size_bytes": stats[0] if stats else 0,
            "table_count": stats[1] if stats else 0,
            "response_time_ms": response_time,
            "pool_configured": database in connection_pools
        }
        
        # Add pool stats if available
        if database in connection_pools:
            pool_stats = get_pool_stats(database)
            connection_info.update(pool_stats)
        
        return True, "Connection successful", connection_info
        
    except ValueError as e:
        return False, str(e), {}
    except Exception as e:
        return False, f"Connection failed: {str(e)}", {}

@app.on_event("startup")
async def startup_event():
    """Initialize connection pools on startup"""
    logger.info("Starting Multi-Database MCP Server...")
    await initialize_connection_pools()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connection pools on shutdown"""
    logger.info("Shutting down Multi-Database MCP Server...")
    await close_connection_pools()

@app.post("/mcp")
async def mcp_handler(
    req: Request, 
    authenticated: bool = Depends(verify_auth)
):
    """Enhanced MCP handler with better error handling and new features"""
    try:
        body = await req.json()
        rpc = JsonRpcRequest(**body)
        method = rpc.method
        params = rpc.params
        
    except Exception as e:
        return {
            "jsonrpc": "2.0", 
            "error": {"code": -32600, "message": f"Invalid request: {str(e)}"}, 
            "id": None
        }

    try:
        if method == "fetch_data":
            query = params.get("query")
            database = params.get("database", "primary")
            limit = params.get("limit", None)
            
            if not query:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "query parameter is required"}, 
                    "id": rpc.id
                }
            
            # Add limit if specified
            if limit and not query.upper().strip().endswith(('LIMIT', 'LIMIT;')):
                query = f"{query.rstrip(';')} LIMIT {limit}"
            
            rows = run_query(query, database)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "rows": rows,
                    "database": database,
                    "row_count": len(rows),
                    "query": query
                }
            }

        elif method == "execute_query":
            query = params.get("query")
            database = params.get("database", "primary")
            
            if not query:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "query parameter is required"}, 
                    "id": rpc.id
                }
            
            row_count = run_exec(query, database)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "row_count": row_count,
                    "database": database,
                    "query": query
                }
            }

        elif method == "get_table_names":
            database = params.get("database", "primary")
            schema_filter = params.get("schema", None)
            
            # Test connection first
            connected, message, _ = test_database_connection(database)
            if not connected:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Database connection failed: {message}"}, 
                    "id": rpc.id
                }
            
            try:
                table_query = """
                SELECT 
                    table_name, 
                    table_type, 
                    table_schema,
                    (SELECT COUNT(*) FROM information_schema.columns c 
                     WHERE c.table_name = t.table_name AND c.table_schema = t.table_schema) as column_count
                FROM information_schema.tables t
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                """
                
                if schema_filter:
                    table_query += f" AND table_schema = '{schema_filter}'"
                
                table_query += " ORDER BY table_schema, table_name"
                
                tables = run_query(table_query, database)
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database": database,
                        "tables": tables,
                        "table_count": len(tables),
                        "schema_filter": schema_filter
                    }
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to fetch tables: {str(e)}"}, 
                    "id": rpc.id
                }

        elif method == "get_table_schema":
            database = params.get("database", "primary")
            table_name = params.get("table_name")
            include_indexes = params.get("include_indexes", False)
            include_constraints = params.get("include_constraints", False)
            
            if not table_name:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "table_name parameter is required"}, 
                    "id": rpc.id
                }
            
            try:
                # Get column information
                schema_query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position,
                    udt_name
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY ordinal_position
                """
                columns = run_query_with_params(schema_query, [table_name], database)
                
                if not columns:
                    return {
                        "jsonrpc": "2.0", 
                        "error": {"code": -32000, "message": f"Table '{table_name}' not found"}, 
                        "id": rpc.id
                    }
                
                result = {
                    "database": database,
                    "table_name": table_name,
                    "columns": columns,
                    "column_count": len(columns)
                }
                
                # Get indexes if requested
                if include_indexes:
                    index_query = """
                    SELECT 
                        i.indexname as index_name,
                        i.indexdef as index_definition,
                        CASE WHEN i.indexdef LIKE '%UNIQUE%' THEN true ELSE false END as is_unique
                    FROM pg_indexes i
                    WHERE i.tablename = %s
                    AND i.schemaname NOT IN ('information_schema', 'pg_catalog')
                    """
                    indexes = run_query_with_params(index_query, [table_name], database)
                    result["indexes"] = indexes
                    result["index_count"] = len(indexes)
                
                # Get constraints if requested
                if include_constraints:
                    constraint_query = """
                    SELECT 
                        tc.constraint_name,
                        tc.constraint_type,
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints tc
                    LEFT JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                    LEFT JOIN information_schema.constraint_column_usage ccu 
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_name = %s
                    """
                    constraints = run_query_with_params(constraint_query, [table_name], database)
                    result["constraints"] = constraints
                    result["constraint_count"] = len(constraints)
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": result
                }
                
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to fetch table schema: {str(e)}"}, 
                    "id": rpc.id
                }

        elif method == "search_across_databases":
            search_term = params.get("search_term")
            databases = params.get("databases") or list(DATABASE_CONFIGS.keys())
            table_pattern = params.get("table_pattern", None)
            max_results_per_table = params.get("max_results_per_table", 5)
            case_sensitive = params.get("case_sensitive", False)
            
            if not search_term:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "search_term parameter is required"}, 
                    "id": rpc.id
                }
            
            results = []
            search_operator = "LIKE" if case_sensitive else "ILIKE"
            
            for db in databases:
                if db not in DATABASE_CONFIGS:
                    results.append({
                        "database": db,
                        "error": f"Database '{db}' not configured",
                        "results": []
                    })
                    continue
                
                try:
                    # Test connection first
                    connected, conn_message, _ = test_database_connection(db)
                    if not connected:
                        results.append({
                            "database": db,
                            "error": f"Connection failed: {conn_message}",
                            "results": []
                        })
                        continue
                    
                    db_results = []
                    
                    # Get tables to search
                    if table_pattern:
                        search_query = """
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_name ILIKE %s 
                        AND table_schema NOT IN ('information_schema', 'pg_catalog')
                        """
                        tables = run_query_with_params(search_query, [f'%{table_pattern}%'], db)
                    else:
                        tables_query = """
                        SELECT DISTINCT t.table_name
                        FROM information_schema.tables t
                        JOIN information_schema.columns c ON t.table_name = c.table_name
                        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
                        AND c.data_type IN ('text', 'varchar', 'character varying', 'char', 'json', 'jsonb')
                        LIMIT 50
                        """
                        tables = run_query(tables_query, db)
                    
                    # Search in each table
                    for table_info in tables:
                        table_name = table_info['table_name']
                        try:
                            # Get searchable columns
                            col_query = """
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_name = %s 
                            AND data_type IN ('text', 'varchar', 'character varying', 'char', 'json', 'jsonb')
                            """
                            columns = run_query_with_params(col_query, [table_name], db)
                            
                            if columns:
                                col_names = [col['column_name'] for col in columns]
                                conditions = ' OR '.join([f'"{col}"::text {search_operator} %s' for col in col_names])
                                search_params = [f'%{search_term}%'] * len(col_names)
                                
                                data_query = f'SELECT * FROM "{table_name}" WHERE {conditions} LIMIT {max_results_per_table}'
                                table_results = run_query_with_params(data_query, search_params, db)
                                
                                if table_results:
                                    db_results.append({
                                        "table": table_name,
                                        "matches": table_results,
                                        "match_count": len(table_results),
                                        "searched_columns": col_names
                                    })
                        except Exception as table_error:
                            logger.warning(f"Error searching table {table_name} in {db}: {str(table_error)}")
                            continue
                    
                    results.append({
                        "database": db,
                        "results": db_results,
                        "tables_searched": len(tables),
                        "matches_found": len(db_results)
                    })
                        
                except Exception as e:
                    results.append({
                        "database": db,
                        "error": str(e),
                        "results": []
                    })
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "search_term": search_term,
                    "databases_searched": databases,
                    "case_sensitive": case_sensitive,
                    "results": results,
                    "total_matches": sum(r.get("matches_found", 0) for r in results)
                }
            }

        elif method == "list_databases":
            # Test connections and get detailed info for all databases
            db_status = {}
            for db_name in DATABASE_CONFIGS.keys():
                connected, message, info = test_database_connection(db_name)
                config = DATABASE_CONFIGS[db_name]
                
                db_status[db_name] = {
                    "connected": connected,
                    "message": message,
                    "url_configured": bool(config["url"]),
                    "description": config["description"],
                    "pool_size": config["pool_size"],
                    "max_overflow": config["max_overflow"],
                    "timeout": config["timeout"],
                    "pool_available": db_name in connection_pools,
                    **info  # Include connection info if available
                }
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "databases": db_status,
                    "count": len(DATABASE_CONFIGS),
                    "pools_initialized": len(connection_pools)
                }
            }

        elif method == "test_connection":
            database = params.get("database", "primary")
            connected, message, info = test_database_connection(database)
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "database": database,
                    "connected": connected,
                    "message": message,
                    **info
                }
            }

        elif method == "get_database_stats":
            database = params.get("database", "primary")
            
            try:
                stats_query = """
                SELECT 
                    current_database() as database_name,
                    pg_database_size(current_database()) as size_bytes,
                    (SELECT count(*) FROM information_schema.tables 
                     WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as table_count,
                    (SELECT count(*) FROM information_schema.views 
                     WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as view_count,
                    (SELECT setting FROM pg_settings WHERE name = 'max_connections') as max_connections,
                    (SELECT count(*) FROM pg_stat_activity) as active_connections,
                    version() as version
                """
                
                stats = run_query(stats_query, database)
                
                # Get top 10 largest tables
                table_sizes_query = """
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC 
                LIMIT 10
                """
                
                table_sizes = run_query(table_sizes_query, database)
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database": database,
                        "stats": stats[0] if stats else {},
                        "largest_tables": table_sizes,
                        "timestamp": datetime.now().isoformat()
                    }
                }
                
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to get database stats: {str(e)}"}, 
                    "id": rpc.id
                }

        else:
            available_methods = [
                "fetch_data", "execute_query", "get_table_names", "get_table_schema",
                "search_across_databases", "list_databases", "test_connection", "get_database_stats"
            ]
            return {
                "jsonrpc": "2.0", 
                "error": {
                    "code": -32601, 
                    "message": f"Method '{method}' not found. Available methods: {available_methods}"
                }, 
                "id": rpc.id
            }

    except Exception as e:
        logger.error(f"Error processing method {method}: {str(e)}")
        return {
            "jsonrpc": "2.0", 
            "error": {"code": -32000, "message": str(e)}, 
            "id": rpc.id
        }

def run_query(query: str, database: str = "primary"):
    """Execute a query on the specified database"""
    try:
        with get_sync_connection(database) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description:
                    colnames = [desc[0] for desc in cur.description]
                    return [dict(zip(colnames, row)) for row in cur.fetchall()]
                else:
                    return []
    except Exception as e:
        logger.error(f"Query execution error on {database}: {str(e)}")
        raise

def run_query_with_params(query: str, params: list, database: str = "primary"):
    """Execute a parameterized query on the specified database"""
    try:
        with get_sync_connection(database) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    colnames = [desc[0] for desc in cur.description]
                    return [dict(zip(colnames, row)) for row in cur.fetchall()]
                else:
                    return []
    except Exception as e:
        logger.error(f"Parameterized query execution error on {database}: {str(e)}")
        raise

def run_exec(query: str, database: str = "primary"):
    """Execute a command on the specified database"""
    try:
        with get_sync_connection(database) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.rowcount
    except Exception as e:
        logger.error(f"Command execution error on {database}: {str(e)}")
        raise

# Enhanced health check endpoint
@app.get("/health")
async def health_check():
    """Check the health of all configured databases with detailed metrics"""
    health_status = {}
    overall_status = "healthy"
    healthy_count = 0
    
    if not DATABASE_CONFIGS:
        return {
            "status": "error",
            "message": "No databases configured",
            "databases": {},
            "timestamp": datetime.now().isoformat()
        }
    
    for db_name in DATABASE_CONFIGS.keys():
        connected, message, info = test_database_connection(db_name)
        config = DATABASE_CONFIGS[db_name]
        
        health_status[db_name] = {
            "connected": connected,
            "message": message,
            "description": config["description"],
            "pool_available": db_name in connection_pools,
            **info
        }
        
        if connected:
            healthy_count += 1
        else:
            overall_status = "degraded" if healthy_count > 0 else "unhealthy"
    
    return {
        "status": overall_status, 
        "databases": health_status,
        "total_databases": len(DATABASE_CONFIGS),
        "healthy_databases": healthy_count,
        "configured_databases": list(DATABASE_CONFIGS.keys()),
        "pools_initialized": len(connection_pools),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/")
async def root():
    """Root endpoint with comprehensive server info"""
    return {
        "message": "Enhanced Multi-Database MCP Server",
        "version": "3.0.0",
        "configured_databases": list(DATABASE_CONFIGS.keys()),
        "active_pools": len(connection_pools),
        "available_methods": [
            "fetch_data", "execute_query", "get_table_names", "get_table_schema",
            "search_across_databases", "list_databases", "test_connection", "get_database_stats"
        ],
        "endpoints": {
            "mcp": "/mcp",
            "health": "/health",
            "docs": "/docs",
            "redoc": "/redoc"
        },
        "features": [
            "Connection pooling",
            "Enhanced authentication",
            "Detailed database statistics",
            "Cross-database search",
            "Schema introspection",
            "Health monitoring"
        ],
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Enhanced Multi-Database MCP Server...")
    print(f"📊 Version: 3.0.0")
    
    # Print detected database configurations  
    print(f"\n🔍 Scanning .env file for POSTGRES_URL_* variables...")
    
    if not DATABASE_CONFIGS:
        print("⚠️  WARNING: No databases configured!")
        print("\nTo add databases, add variables to your .env file like:")
        print("POSTGRES_URL_PRIMARY=postgresql://user:password@host:5432/database")
    else:
        print(f"✅ Found {len(DATABASE_CONFIGS)} database configurations:")
        
        # Show discovered databases
        for db_key, config in DATABASE_CONFIGS.items():
            print(f"   • {db_key} → {config['description']}")
            print(f"     Pool: {config['pool_size']}, Timeout: {config['timeout']}s")
        
        # Test connections on startup
        print("\n🔍 Testing database connections...")
        for db_name in DATABASE_CONFIGS.keys():
            connected, message, info = test_database_connection(db_name)
            status = "✅" if connected else "❌"
            description = DATABASE_CONFIGS[db_name]["description"]
            print(f"{status} {db_name} ({description}): {message}")
            if connected and info:
                print(f"    📈 Response time: {info.get('response_time_ms', 'N/A')}ms")
                print(f"    🗃️  Tables: {info.get('table_count', 'N/A')}")
                print(f"    💾 Size: {info.get('database_size_bytes', 0):,} bytes")
    
    print(f"\n🔐 Authentication: {'Enabled' if AUTH_TOKEN else 'Disabled'}")
    print("🌐 Server starting on http://0.0.0.0:8000")
    print("📚 API Documentation: http://0.0.0.0:8000/docs")
    print("🏥 Health Check: http://0.0.0.0:8000/health")
    print(f"\n💡 Tip: Add more databases by adding POSTGRES_URL_YOURNAME variables to .env")
    print("🧪 To test your setup, run: python database_tester.py")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    )
