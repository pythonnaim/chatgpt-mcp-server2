from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
import psycopg
import os
import json
from dotenv import load_dotenv
from typing import Dict, Optional

load_dotenv()  # Load DB creds from .env

app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN")

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
        return {}

# Add JSON configs if file exists
DATABASE_CONFIGS.update(load_database_configs())

class DatabaseRegistry:
    def __init__(self):
        self.databases = {k: v for k, v in DATABASE_CONFIGS.items() if v}  # Only keep configured databases
    
    def add_database(self, name: str, connection_string: str):
        """Add a new database configuration"""
        self.databases[name] = connection_string
    
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

# Initialize database registry
db_registry = DatabaseRegistry()

class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: int
    params: dict

@app.post("/mcp")
async def mcp_handler(req: Request, authorization: str = Header(None)):
    # Uncomment to enable authentication
    # if authorization != f"Bearer {AUTH_TOKEN}":
    #     raise HTTPException(status_code=401, detail="Unauthorized")

    body = await req.json()
    try:
        rpc = JsonRpcRequest(**body)
        method = rpc.method
        params = rpc.params
        query = params.get("query")
        database = params.get("database", "primary")  # Default to primary database
    except Exception as e:
        return {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}, "id": None}

    try:
        if method == "fetch_data":
            rows = run_query(query, database)
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
            row_count = run_exec(query, database)
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
            except Exception as e:
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
            return {
                "jsonrpc": "2.0", 
                "error": {
                    "code": -32601, 
                    "message": f"Method '{method}' not found. Available methods: {available_methods}"
                }, 
                "id": rpc.id
            }

    except Exception as e:
        return {
            "jsonrpc": "2.0", 
            "error": {
                "code": -32000, 
                "message": str(e),
                "database": database if 'database' in locals() else None
            }, 
            "id": rpc.id
        }


def run_query(query: str, database: str = "primary"):
    """Execute a SELECT query on the specified database"""
    db_url = db_registry.get_database_url(database)
    
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description:  # Has results
                colnames = [desc[0] for desc in cur.description]
                return [dict(zip(colnames, row)) for row in cur.fetchall()]
            else:
                return []

def run_exec(query: str, database: str = "primary"):
    """Execute an INSERT/UPDATE/DELETE query on the specified database"""
    db_url = db_registry.get_database_url(database)
    
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount

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
                
                return {
                    "version": version,
                    "database_name": db_name,
                    "current_user": user,
                    "server_time": str(timestamp),
                    "connection_url": db_url.split('@')[1] if '@' in db_url else "hidden"  # Hide credentials
                }
    except Exception as e:
        return {"error": str(e)}

# Health check endpoint
@app.get("/health")
async def health_check():
    """Check the health of all configured databases"""
    health_status = {}
    overall_status = "healthy"
    
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
        "total_databases": len(db_registry.databases)
    }

# Info endpoint
@app.get("/info")
async def server_info():
    """Get server information"""
    return {
        "server": "Multi-Database MCP Server",
        "version": "1.0",
        "available_databases": list(db_registry.databases.keys()),
        "available_methods": [
            "fetch_data", "execute_query", "list_databases", 
            "add_database", "remove_database", "test_connection", 
            "get_database_info"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    print("Starting Multi-Database MCP Server...")
    print(f"Available databases: {list(db_registry.databases.keys())}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
