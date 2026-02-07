import os
from pathlib import Path

def get_db_properties():
    def get_secret(name, default=None):
        project_root = Path(__file__).resolve().parents[2] 
        local_secret_path = project_root / "secrets" / f"{name}.txt"
        docker_secret_path = Path(f"/run/secrets/{name}")
        
        if docker_secret_path.exists():
             with open(docker_secret_path, "r") as f:
                return f.read().strip()
        elif local_secret_path.exists():
            with open(local_secret_path, "r") as f:
                return f.read().strip()
        else:
            return os.getenv(name.upper(), default)

    db_host = os.getenv("DB_HOST", "postgres")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "warehouse_db")
    db_user = os.getenv("DB_USER", "admin_user")
    db_pass = get_secret("warehouse_password", "admin_password")

    return {
        "url": f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?stringtype=unspecified",
        "user": db_user,
        "password": db_pass,
        "driver": "org.postgresql.Driver"
    }