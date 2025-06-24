from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
import psycopg
import os
import json
import logging
import traceback
from dotenv import load_dotenv
from typing import Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()  # Load DB creds from .env

app = FastAPI(title="Multi-Database MCP Server", version="1.0.0")

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN")
ENABLE_AUTH = os.getenv("ENABLE_AUTH", "false").lower() == "true"

# Multiple database configurations from environment variables
DATABASE_CONFIGS = {
    "primary": os.getenv("POSTGRES_URL_PRIMARY") or os.getenv("POSTGRES_URL"),  # Fallback to original
    "analytics": os.getenv("POSTGRES_URL_ANALYTICS"), 
    "reporting": os.getenv("POSTGRES_URL_REPORTING"),
    "staging": os.getenv("POSTGRES_URL_STAGING"),
}

# Optional: Load additional databases from JSON file
def load_database_configs():
    """Load database configurations from a JSON file"""
    try:
        with open("database_configs.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.info("No database_configs.json file found, using environment variables only")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in database_configs.json: {e}")
        return {}

# Add JSON configs if file exists
try:
    json_configs = load_database_configs()
    DATABASE_CONFIGS.update(json_configs)
except Exception as e:
    logger.error(f"Error loading database configs: {e}")

class DatabaseRegistry:
    def __init__(self):
        self.databases = {k: v for k, v in DATABASE_CONFIGS.items() if v}  # Only keep configured databases
        logger.info(f"Initialized database registry with: {list(self.databases.keys())}")
        
        # Warn if no databases are configured
        if not self.databases:
            logger.warning("No databases configured! Please set POSTGRES_URL or POSTGRES_URL_PRIMARY environment variable")
    
    def add_database(self, name: str, connection_string: str):
        """Add a new database configuration"""
        self.databases[name] = connection_string
        logger.info(f"Added database: {name}")
    
    def get_database_url(self, name: str) -> str:
        """Get database URL by name"""
        if name not in self.databases:
            available = list(self.databases.keys())
            raise ValueError(f"Database '{name}' not found. Available databases: {available}")
        if not self.databases[name]:
            raise ValueError(f"Database '{name}' URL is not configured")
        return self.databases[name]
    
    def list_databases(self) -> Dict[str, bool]:
        """List all available databases and their availability"""
        return {name: bool(url) for name, url in self.databases.items()}
    
    def remove_database(self, name: str):
        """Remove a database from registry"""
        if name in self.databases:
            del self.databases[name]
            logger.info(f"Removed database: {name}")

# Initialize database registry
db_registry = DatabaseRegistry()

class JsonRpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    id: int
    params: dict = {}

def authenticate_request(authorization: Optional[str] = None) -> bool:
    """Check if request is authenticated"""
    if not ENABLE_AUTH:
        return True
    
    if not AUTH_TOKEN:
        logger.warning("AUTH_TOKEN not set but authentication is enabled")
        return False
        
    expected_header = f"Bearer {AUTH_TOKEN}"
    return authorization == expected_header

@app.post("/mcp")
async def mcp_handler(req: Request, authorization: str = Header(None)):
    """Main MCP JSON-RPC handler"""
    
    # Authentication check
    if not authenticate_request(authorization):
        logger.warning(f"Unauthorized request from {req.client.host if req.client else 'unknown'}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await req.json()
        logger.info(f"Received request: {body.get('method', 'unknown')}")
    except Exception as e:
        logger.error(f"Invalid JSON in request: {e}")
        return {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None}

    try:
        rpc = JsonRpcRequest(**body)
        method = rpc.method
        params = rpc.params
        query = params.get("query")
        database = params.get("database", "primary")  # Default to primary database
        
        logger.info(f"Processing method: {method}, database: {database}")
        
    except Exception as e:
        logger.error(f"Invalid request format: {e}")
        return {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}, "id": None}

    try:
        if method == "fetch_data":
            if not query:
                raise ValueError("Query parameter is required for fetch_data method")
            
            rows = run_query(query, database)
            logger.info(f"Query executed successfully, returned {len(rows)} rows")
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "rows": rows, 
                    "database": database,
                    "row_count": len(rows)
                }
            }

        elif method == "execute_query":
            if not query:
                raise ValueError("Query parameter is required for execute_query method")
                
            row_count = run_exec(query, database)
            logger.info(f"Query executed successfully, affected {row_count} rows")
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "row_count": row_count, 
                    "database": database
                }
            }
        
        elif method == "list_databases":
            databases = db_registry.list_databases()
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "databases": databases,
                    "count": len(databases)
                }
            }
        
        elif method == "add_database":
            name = params.get("name")
            connection_string = params.get("connection_string")
            if not name or not connection_string:
                raise ValueError("Both 'name' and 'connection_string' are required")
            
            # Test connection before adding
            try:
                with psycopg.connect(connection_string, autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        logger.info(f"Connection test successful for database: {name}")
            except Exception as e:
                logger.error(f"Connection test failed for database {name}: {e}")
                raise ValueError(f"Failed to connect to database: {str(e)}")
            
            db_registry.add_database(name, connection_string)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "message": f"Database '{name}' added successfully",
                    "database": name
                }
            }
        
        elif method == "remove_database":
            name = params.get("name")
            if not name:
                raise ValueError("Database 'name' is required")
            if name == "primary":
                raise ValueError("Cannot remove primary database")
            
            db_registry.remove_database(name)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "message": f"Database '{name}' removed successfully",
                    "database": name
                }
            }
        
        elif method == "test_connection":
            database = params.get("database", "primary")
            success, message = test_database_connection(database)
            logger.info(f"Connection test for {database}: {message}")
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "database": database, 
                    "connected": success,
                    "message": message
                }
            }
        
        elif method == "get_database_info":
            database = params.get("database", "primary")
            info = get_database_info(database)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "database": database,
                    "info": info
                }
            }

        else:
            available_methods = [
                "fetch_data", "execute_query", "list_databases", 
                "add_database", "remove_database", "test_connection", 
                "get_database_info"
            ]
            logger.warning(f"Unknown method requested: {method}")
            return {
                "jsonrpc": "2.0", 
                "error": {
                    "code": -32601, 
                    "message": f"Method '{method}' not found. Available methods: {available_methods}"
                }, 
                "id": rpc.id
            }

    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error processing {method}: {error_details}")
        
        return {
            "jsonrpc": "2.0", 
            "error": {
                "code": -32000, 
                "message": str(e),
                "database": database if 'database' in locals() else None,
                "method": method if 'method' in locals() else None
            }, 
            "id": rpc.id if 'rpc' in locals() else None
        }


def run_query(query: str, database: str = "primary"):
    """Execute a SELECT query on the specified database"""
    logger.info(f"Executing query on {database}: {query[:100]}...")
    
    try:
        db_url = db_registry.get_database_url(database)
        
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description:  # Has results
                    colnames = [desc[0] for desc in cur.description]
                    rows = [dict(zip(colnames, row)) for row in cur.fetchall()]
                    logger.info(f"Query returned {len(rows)} rows with columns: {colnames}")
                    return rows
                else:
                    logger.info("Query executed but returned no results")
                    return []
                    
    except Exception as e:
        logger.error(f"Error executing query on {database}: {e}")
        raise

def run_exec(query: str, database: str = "primary"):
    """Execute an INSERT/UPDATE/DELETE query on the specified database"""
    logger.info(f"Executing exec query on {database}: {query[:100]}...")
    
    try:
        db_url = db_registry.get_database_url(database)
        
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row_count = cur.rowcount
                logger.info(f"Query affected {row_count} rows")
                return row_count
                
    except Exception as e:
        logger.error(f"Error executing exec query on {database}: {e}")
        raise

def test_database_connection(database: str = "primary") -> tuple[bool, str]:
    """Test if a database connection is working"""
    try:
        db_url = db_registry.get_database_url(database)
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True, "Connection successful"
    except ValueError as e:
        return False, str(e)
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False, f"Connection failed: {str(e)}"

def get_database_info(database: str = "primary") -> dict:
    """Get basic information about the database"""
    try:
        db_url = db_registry.get_database_url(database)
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                # Get database version
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                
                # Get current database name
                cur.execute("SELECT current_database()")
                db_name = cur.fetchone()[0]
                
                # Get current user
                cur.execute("SELECT current_user")
                user = cur.fetchone()[0]
                
                # Get current timestamp
                cur.execute("SELECT NOW()")
                timestamp = cur.fetchone()[0]
                
                # Hide credentials in URL
                safe_url = db_url.split('@')[1] if '@' in db_url else "hidden"
                
                return {
                    "version": version,
                    "database_name": db_name,
                    "current_user": user,
                    "server_time": str(timestamp),
                    "connection_url": safe_url
                }
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        return {"error": str(e)}

# Health check endpoint
@app.get("/health")
async def health_check():
    """Check the health of all configured databases"""
    health_status = {}
    overall_status = "healthy"
    
    if not db_registry.databases:
        return {
            "status": "error",
            "message": "No databases configured",
            "databases": {},
            "total_databases": 0
        }
    
    for db_name in db_registry.databases.keys():
        connected, message = test_database_connection(db_name)
        health_status[db_name] = {
            "connected": connected,
            "message": message
        }
        if not connected:
            overall_status = "degraded"
    
    return {
        "status": overall_status, 
        "databases": health_status,
        "total_databases": len(db_registry.databases),
        "auth_enabled": ENABLE_AUTH
    }

# Info endpoint
@app.get("/info")
async def server_info():
    """Get server information"""
    return {
        "server": "Multi-Database MCP Server",
        "version": "1.0.0",
        "available_databases": list(db_registry.databases.keys()),
        "database_count": len(db_registry.databases),
        "available_methods": [
            "fetch_data", "execute_query", "list_databases", 
            "add_database", "remove_database", "test_connection", 
            "get_database_info"
        ],
        "auth_enabled": ENABLE_AUTH,
        "environment": {
            "has_primary_db": bool(db_registry.databases.get("primary")),
            "total_env_vars": len([k for k in os.environ.keys() if k.startswith("POSTGRES_URL")])
        }
    }

# Root endpoint for basic info
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Multi-Database MCP Server is running",
        "version": "1.0.0",
        "endpoints": {
            "mcp": "/mcp (POST) - Main JSON-RPC endpoint",
            "health": "/health (GET) - Health check",
            "info": "/info (GET) - Server information"
        }
    }

if __name__ == "__main__":
    import uvicorn
    print("=" * 50)
    print("Starting Multi-Database MCP Server...")
    print(f"Authentication: {'Enabled' if ENABLE_AUTH else 'Disabled'}")
    print(f"Available databases: {list(db_registry.databases.keys())}")
    
    if not db_registry.databases:
        print("⚠️  WARNING: No databases configured!")
        print("Please set POSTGRES_URL or POSTGRES_URL_PRIMARY environment variable")
    
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8000)
