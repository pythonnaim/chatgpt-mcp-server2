from fastapi import FastAPI, Request, HTTPException
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
    version="3.0.1"
)

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dynamically load database configurations from environment variables
def load_database_configs():
    """
    Dynamically load database configurations from .env file
    Looks for POSTGRES_URL_* environment variables where * becomes the database name
    Also supports DB_URL_* format for compatibility
    """
    configs = {}
    
    # Get all environment variables
    env_vars = os.environ
    
    # Find all POSTGRES_URL_* and DB_URL_* variables
    for key, value in env_vars.items():
        db_name = None
        
        if key.startswith("POSTGRES_URL_") and value:
            # Extract database name from environment variable
            # POSTGRES_URL_PRIMARY -> primary (convert to lowercase)
            db_name = key.replace("POSTGRES_URL_", "").lower()
        elif key.startswith("DB_URL_") and value:
            # Extract database name from DB_URL_* format
            # DB_URL_SCHOOLSTATUS_CODE -> schoolstatus_code
            db_name = key.replace("DB_URL_", "").lower()
        
        if db_name and value:
            # Default configuration values - no special treatment for "primary"
            default_pool_size = "10"  # All databases get the same pool size
            default_timeout = "60"    # All databases get the same timeout
            
            # Build configuration for this database
            configs[db_name] = {
                "url": value,
                "pool_size": int(os.getenv(f"POSTGRES_POOL_SIZE_{db_name.upper()}", default_pool_size)),
                "max_overflow": int(os.getenv(f"POSTGRES_MAX_OVERFLOW_{db_name.upper()}", "20")),
                "timeout": int(os.getenv(f"POSTGRES_TIMEOUT_{db_name.upper()}", default_timeout)),
                "description": db_name.replace("_", " ").title()
            }
    
    return configs

# Load database configurations dynamically
DATABASE_CONFIGS = load_database_configs()

# Connection pools storage - simple implementation
connection_pools: Dict[str, Queue] = {}
pool_locks: Dict[str, threading.Lock] = {}

class PersistentConnectionPool:
    """Persistent connection pool that maintains connections and auto-reconnects"""
    
    def __init__(self, database_url: str, pool_size: int = 10, timeout: int = 60):
        self.database_url = database_url
        self.pool_size = pool_size
        self.timeout = timeout
        self.pool = Queue(maxsize=pool_size)
        self.lock = threading.Lock()
        self.total_connections = 0
        self.failed_connections = 0
        self.is_healthy = True
        
        # Pre-fill the pool and keep connections alive
        self._initialize_pool()
        
        # Start background thread to maintain connections
        self.maintenance_thread = threading.Thread(target=self._maintain_connections, daemon=True)
        self.maintenance_thread.start()
    
    def _initialize_pool(self):
        """Initialize the connection pool with all connections"""
        logger.info(f"Initializing persistent pool with {self.pool_size} connections...")
        for i in range(self.pool_size):
            try:
                conn = psycopg.connect(self.database_url)
                conn.autocommit = True
                self.pool.put(conn)
                self.total_connections += 1
                logger.debug(f"Created connection {i+1}/{self.pool_size}")
            except Exception as e:
                logger.error(f"Failed to create connection {i+1}: {e}")
                self.failed_connections += 1
                break
        
        self.is_healthy = self.total_connections > 0
        logger.info(f"Pool initialized: {self.total_connections} active connections")
    
    def _maintain_connections(self):
        """Background thread to maintain connections"""
        while True:
            try:
                time.sleep(30)  # Check every 30 seconds
                self._health_check()
            except Exception as e:
                logger.error(f"Connection maintenance error: {e}")
    
    def _health_check(self):
        """Check and repair connections in the pool"""
        with self.lock:
            healthy_connections = []
            
            # Check all existing connections
            while not self.pool.empty():
                try:
                    conn = self.pool.get(block=False)
                    # Test the connection
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    healthy_connections.append(conn)
                except Exception:
                    # Connection is dead
                    try:
                        conn.close()
                    except:
                        pass
                    self.total_connections -= 1
            
            # Put healthy connections back
            for conn in healthy_connections:
                self.pool.put(conn)
            
            # Create new connections if needed
            missing_connections = self.pool_size - len(healthy_connections)
            for _ in range(missing_connections):
                try:
                    conn = psycopg.connect(self.database_url)
                    conn.autocommit = True
                    self.pool.put(conn)
                    self.total_connections += 1
                except Exception as e:
                    logger.warning(f"Failed to create replacement connection: {e}")
                    break
            
            self.is_healthy = self.total_connections > 0
            if missing_connections > 0:
                logger.info(f"Repaired {missing_connections} connections. Active: {self.total_connections}")
    
    def get_connection(self):
        """Get a connection from the pool"""
        if not self.is_healthy:
            raise Exception("Connection pool is unhealthy")
        
        try:
            # Try to get an existing connection
            conn = self.pool.get(timeout=5)
            
            # Quick test if connection is still alive
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return conn
            except Exception:
                # Connection is dead, get another one
                try:
                    conn.close()
                except:
                    pass
                self.total_connections -= 1
                # Try to get another connection
                return self.get_connection()
                
        except Empty:
            # No connections available, create a temporary one
            try:
                conn = psycopg.connect(self.database_url)
                conn.autocommit = True
                return conn
            except Exception as e:
                logger.error(f"Failed to create temporary connection: {e}")
                raise Exception("No connections available and failed to create new one")
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        try:
            if not conn.closed and not self.pool.full():
                self.pool.put(conn, block=False)
            else:
                # Pool is full or connection is bad, close it
                try:
                    conn.close()
                except:
                    pass
                if not conn.closed:
                    self.total_connections -= 1
        except Exception:
            # Couldn't return to pool, close it
            try:
                conn.close()
            except:
                pass
    
    def close_all(self):
        """Close all connections in the pool"""
        with self.lock:
            while not self.pool.empty():
                try:
                    conn = self.pool.get(block=False)
                    conn.close()
                except:
                    pass
            self.total_connections = 0
            self.is_healthy = False
    
    def get_stats(self):
        """Get pool statistics"""
        return {
            "total_connections": self.total_connections,
            "available_connections": self.pool.qsize(),
            "max_connections": self.pool_size,
            "failed_connections": self.failed_connections,
            "is_healthy": self.is_healthy
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

async def initialize_connection_pools():
    """Initialize persistent connection pools for all configured databases"""
    global connection_pools
    
    if not DATABASE_CONFIGS:
        logger.warning("No database configurations found!")
        return
    
    for db_name, config in DATABASE_CONFIGS.items():
        try:
            logger.info(f"Initializing persistent connection pool for {db_name}...")
            
            pool = PersistentConnectionPool(
                config["url"],
                pool_size=config["pool_size"],
                timeout=config["timeout"]
            )
            
            if pool.is_healthy:
                connection_pools[db_name] = pool
                logger.info(f"✅ Persistent connection pool initialized for {db_name}")
            else:
                logger.error(f"❌ Failed to initialize healthy pool for {db_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize pool for {db_name}: {str(e)}")

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
def get_sync_connection(database: str):
    """Get a synchronous database connection with proper error handling and pooling"""
    if not database:
        # If no database specified, use the first available one
        if DATABASE_CONFIGS:
            database = list(DATABASE_CONFIGS.keys())[0]
            logger.info(f"No database specified, using first available: {database}")
        else:
            raise ValueError("No database specified and no databases configured")
    
    if database not in DATABASE_CONFIGS:
        available = list(DATABASE_CONFIGS.keys())
        raise ValueError(f"Database '{database}' not found. Available: {available}")
    
    db_config = DATABASE_CONFIGS[database]
    if not db_config["url"]:
        raise ValueError(f"Database '{database}' URL is not configured")
    
    # Try to use persistent connection pool first
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
        # Fallback to direct connection (shouldn't happen with persistent pools)
        logger.warning(f"No pool available for {database}, creating direct connection")
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
async def mcp_handler(req: Request):
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
            database = params.get("database")  # Remove default, make it required
            limit = params.get("limit", None)
            
            if not query:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "query parameter is required"}, 
                    "id": rpc.id
                }
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required. Available databases: " + ", ".join(DATABASE_CONFIGS.keys())}, 
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
            database = params.get("database")  # Remove default, make it required
            
            if not query:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "query parameter is required"}, 
                    "id": rpc.id
                }
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required. Available databases: " + ", ".join(DATABASE_CONFIGS.keys())}, 
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
            database = params.get("database")  # Remove default, make it required
            schema_filter = params.get("schema", None)
            table_type_filter = params.get("table_type", None)  # BASE TABLE, VIEW, etc.
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required. Available databases: " + ", ".join(DATABASE_CONFIGS.keys())}, 
                    "id": rpc.id
                }
            
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
                
                query_params = []
                if schema_filter:
                    table_query += " AND table_schema = %s"
                    query_params.append(schema_filter)
                
                if table_type_filter:
                    table_query += " AND table_type = %s"
                    query_params.append(table_type_filter)
                
                table_query += " ORDER BY table_schema, table_name"
                
                if query_params:
                    tables = run_query_with_params(table_query, query_params, database)
                else:
                    tables = run_query(table_query, database)
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database": database,
                        "tables": tables,
                        "table_count": len(tables),
                        "schema_filter": schema_filter,
                        "table_type_filter": table_type_filter
                    }
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to fetch tables: {str(e)}"}, 
                    "id": rpc.id
                }

        elif method == "get_all_tables":
            """Get tables from all databases or specific databases"""
            databases = params.get("databases", list(DATABASE_CONFIGS.keys()))
            schema_filter = params.get("schema", None)
            table_type_filter = params.get("table_type", None)
            
            if isinstance(databases, str):
                databases = [databases]
            
            results = []
            
            for db in databases:
                if db not in DATABASE_CONFIGS:
                    results.append({
                        "database": db,
                        "error": f"Database '{db}' not configured",
                        "tables": []
                    })
                    continue
                
                try:
                    # Test connection first
                    connected, conn_message, _ = test_database_connection(db)
                    if not connected:
                        results.append({
                            "database": db,
                            "error": f"Connection failed: {conn_message}",
                            "tables": []
                        })
                        continue
                    
                    # Get tables from this database
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
                    
                    query_params = []
                    if schema_filter:
                        table_query += " AND table_schema = %s"
                        query_params.append(schema_filter)
                    
                    if table_type_filter:
                        table_query += " AND table_type = %s"
                        query_params.append(table_type_filter)
                    
                    table_query += " ORDER BY table_schema, table_name"
                    
                    if query_params:
                        tables = run_query_with_params(table_query, query_params, db)
                    else:
                        tables = run_query(table_query, db)
                    
                    results.append({
                        "database": db,
                        "tables": tables,
                        "table_count": len(tables)
                    })
                    
                except Exception as e:
                    results.append({
                        "database": db,
                        "error": str(e),
                        "tables": []
                    })
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "databases_queried": databases,
                    "schema_filter": schema_filter,
                    "table_type_filter": table_type_filter,
                    "results": results,
                    "total_tables": sum(r.get("table_count", 0) for r in results)
                }
            }

        elif method == "get_table_schema":
            database = params.get("database")  # Remove default, make it required
            table_name = params.get("table_name")
            table_schema = params.get("table_schema", "public")  # Default to public schema
            include_indexes = params.get("include_indexes", False)
            include_constraints = params.get("include_constraints", False)
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required. Available databases: " + ", ".join(DATABASE_CONFIGS.keys())}, 
                    "id": rpc.id
                }
            
            if not table_name:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "table_name parameter is required"}, 
                    "id": rpc.id
                }
            
            try:
                # Get column information with schema
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
                    udt_name,
                    table_schema
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema = %s
                ORDER BY ordinal_position
                """
                columns = run_query_with_params(schema_query, [table_name, table_schema], database)
                
                if not columns:
                    return {
                        "jsonrpc": "2.0", 
                        "error": {"code": -32000, "message": f"Table '{table_schema}.{table_name}' not found"}, 
                        "id": rpc.id
                    }
                
                result = {
                    "database": database,
                    "table_name": table_name,
                    "table_schema": table_schema,
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
                    AND i.schemaname = %s
                    """
                    indexes = run_query_with_params(index_query, [table_name, table_schema], database)
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
                        ccu.column_name AS foreign_column_name,
                        tc.table_schema
                    FROM information_schema.table_constraints tc
                    LEFT JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    LEFT JOIN information_schema.constraint_column_usage ccu 
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_name = %s
                    AND tc.table_schema = %s
                    """
                    constraints = run_query_with_params(constraint_query, [table_name, table_schema], database)
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
                        SELECT table_name, table_schema FROM information_schema.tables 
                        WHERE table_name ILIKE %s 
                        AND table_schema NOT IN ('information_schema', 'pg_catalog')
                        """
                        tables = run_query_with_params(search_query, [f'%{table_pattern}%'], db)
                    else:
                        tables_query = """
                        SELECT DISTINCT t.table_name, t.table_schema
                        FROM information_schema.tables t
                        JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
                        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
                        AND c.data_type IN ('text', 'varchar', 'character varying', 'char', 'json', 'jsonb')
                        LIMIT 50
                        """
                        tables = run_query(tables_query, db)
                    
                    # Search in each table
                    for table_info in tables:
                        table_name = table_info['table_name']
                        table_schema = table_info.get('table_schema', 'public')
                        full_table_name = f'"{table_schema}"."{table_name}"'
                        
                        try:
                            # Get searchable columns
                            col_query = """
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_name = %s 
                            AND table_schema = %s
                            AND data_type IN ('text', 'varchar', 'character varying', 'char', 'json', 'jsonb')
                            """
                            columns = run_query_with_params(col_query, [table_name, table_schema], db)
                            
                            if columns:
                                col_names = [col['column_name'] for col in columns]
                                conditions = ' OR '.join([f'"{col}"::text {search_operator} %s' for col in col_names])
                                search_params = [f'%{search_term}%'] * len(col_names)
                                
                                data_query = f'SELECT * FROM {full_table_name} WHERE {conditions} LIMIT {max_results_per_table}'
                                table_results = run_query_with_params(data_query, search_params, db)
                                
                                if table_results:
                                    db_results.append({
                                        "table": table_name,
                                        "schema": table_schema,
                                        "matches": table_results,
                                        "match_count": len(table_results),
                                        "searched_columns": col_names
                                    })
                        except Exception as table_error:
                            logger.warning(f"Error searching table {full_table_name} in {db}: {str(table_error)}")
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
            database = params.get("database")  # Remove default, make it required
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required. Available databases: " + ", ".join(DATABASE_CONFIGS.keys())}, 
                    "id": rpc.id
                }
            
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

        elif method == "get_database_info":
            """Get detailed information about a specific database including its actual database name"""
            database = params.get("database")  # Remove default, make it required
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required. Available databases: " + ", ".join(DATABASE_CONFIGS.keys())}, 
                    "id": rpc.id
                }
            
            try:
                # Get comprehensive database information
                info_query = """
                SELECT 
                    current_database() as connected_database_name,
                    current_user as connected_user,
                    inet_server_addr() as server_address,
                    inet_server_port() as server_port,
                    version() as postgres_version,
                    pg_database_size(current_database()) as database_size_bytes,
                    (SELECT count(*) FROM information_schema.tables 
                     WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as user_table_count,
                    (SELECT array_agg(DISTINCT table_schema) 
                     FROM information_schema.tables 
                     WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as user_schemas,
                    now() as server_time
                """
                
                db_info = run_query(info_query, database)
                
                # Get list of all schemas with table counts
                schema_query = """
                SELECT 
                    table_schema,
                    count(*) as table_count,
                    array_agg(table_name ORDER BY table_name) as tables
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                GROUP BY table_schema
                ORDER BY table_schema
                """
                
                schemas = run_query(schema_query, database)
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database_config_name": database,
                        "database_info": db_info[0] if db_info else {},
                        "schemas": schemas,
                        "schema_count": len(schemas),
                        "timestamp": datetime.now().isoformat()
                    }
                }
                
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to get database info: {str(e)}"}, 
                    "id": rpc.id
                }

        elif method == "connect_to_database":
            """Explicitly connect to a specific database and return connection details"""
            database = params.get("database")
            
            if not database:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "database parameter is required"}, 
                    "id": rpc.id
                }
            
            try:
                connected, message, info = test_database_connection(database)
                
                if not connected:
                    return {
                        "jsonrpc": "2.0", 
                        "error": {"code": -32000, "message": f"Failed to connect to database '{database}': {message}"}, 
                        "id": rpc.id
                    }
                
                # Get the actual database name and available schemas
                with get_sync_connection(database) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT 
                                current_database() as actual_db_name,
                                array_agg(DISTINCT schema_name) as available_schemas
                            FROM information_schema.schemata 
                            WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
                        """)
                        db_details = cur.fetchone()
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database_config": database,
                        "actual_database_name": db_details[0] if db_details else None,
                        "available_schemas": db_details[1] if db_details else [],
                        "connection_successful": True,
                        "connection_info": info,
                        "message": message
                    }
                }
                
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Error connecting to database: {str(e)}"}, 
                    "id": rpc.id
                }
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
                "fetch_data", "execute_query", "get_table_names", "get_all_tables", "get_table_schema",
                "search_across_databases", "list_databases", "test_connection", "get_database_stats",
                "get_database_info", "connect_to_database"
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

def run_query(query: str, database: str):
    """Execute a query on the specified database"""
    if not database:
        raise ValueError("Database parameter is required")
    
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

def run_query_with_params(query: str, params: list, database: str):
    """Execute a parameterized query on the specified database"""
    if not database:
        raise ValueError("Database parameter is required")
        
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

def run_exec(query: str, database: str):
    """Execute a command on the specified database"""
    if not database:
        raise ValueError("Database parameter is required")
        
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
        "version": "3.0.2",
        "configured_databases": list(DATABASE_CONFIGS.keys()),
        "active_pools": len(connection_pools),
        "available_methods": [
            "fetch_data", "execute_query", "get_table_names", "get_all_tables", "get_table_schema",
            "search_across_databases", "list_databases", "test_connection", "get_database_stats",
            "get_database_info", "connect_to_database"
        ],
        "endpoints": {
            "mcp": "/mcp",
            "health": "/health",
            "docs": "/docs",
            "redoc": "/redoc"
        },
        "features": [
            "Connection pooling",
            "Detailed database statistics",
            "Cross-database search",
            "Schema introspection",
            "Health monitoring",
            "All-database table querying"
        ],
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Enhanced Multi-Database MCP Server...")
    print(f"📊 Version: 3.0.2 - Persistent Connections")
    
    # Print detected database configurations  
    print(f"\n🔍 Scanning .env file for POSTGRES_URL_* and DB_URL_* variables...")
    
    if not DATABASE_CONFIGS:
        print("⚠️  WARNING: No databases configured!")
        print("\nTo add databases, add variables to your .env file like:")
        print("POSTGRES_URL_SCHOOLSTATUS_CODE=postgresql://user:password@host:5432/database")
        print("DB_URL_SCHOOLSTATUS_CODE=postgresql://user:password@host:5432/schoolstatus_code")
        print("\n❌ Cannot start server without database configurations!")
    else:
        print(f"✅ Found {len(DATABASE_CONFIGS)} database configurations:")
        
        # Show discovered databases
        for db_key, config in DATABASE_CONFIGS.items():
            print(f"   • {db_key} → {config['description']}")
            print(f"     Pool: {config['pool_size']}, Timeout: {config['timeout']}s")
        
        # Test connections on startup
        print("\n🔍 Testing database connections...")
        healthy_dbs = 0
        for db_name in DATABASE_CONFIGS.keys():
            connected, message, info = test_database_connection(db_name)
            status = "✅" if connected else "❌"
            description = DATABASE_CONFIGS[db_name]["description"]
            print(f"{status} {db_name} ({description}): {message}")
            if connected:
                healthy_dbs += 1
                if info:
                    print(f"    📈 Response time: {info.get('response_time_ms', 'N/A')}ms")
                    print(f"    🗃️  Tables: {info.get('table_count', 'N/A')}")
                    print(f"    💾 Size: {info.get('database_size_bytes', 0):,} bytes")
        
        
        print(f"\n🎯 Ready to serve {healthy_dbs}/{len(DATABASE_CONFIGS)} databases")
        print("⚡ All connections will remain persistent and auto-reconnect")
        print("📋 Database parameter is now REQUIRED for all queries")
    
    print(f"\n🔐 Authentication: Disabled")
    print("🌐 Server starting on http://0.0.0.0:8000")
    print("📚 API Documentation: http://0.0.0.0:8000/docs")
    print("🏥 Health Check: http://0.0.0.0:8000/health")
    print(f"\n💡 Tip: Add more databases by adding POSTGRES_URL_YOURNAME or DB_URL_YOURNAME variables to .env")
    print("🧪 To test your setup, run: python database_tester.py")
    print("\n🚨 NOTE: 'database' parameter is now REQUIRED for all queries!")
    print(f"Available databases: {', '.join(DATABASE_CONFIGS.keys())}")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=3000,
        log_level="info"
    )
