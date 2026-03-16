from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    database: str
    user: str
    password: str
    raw_csv_dir: Path

def load_settings() -> Settings:
    # Simple env loader (works with .env if you export vars or use python-dotenv)
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    database = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    raw_csv_dir = Path(os.getenv("RAW_CSV_DIR", "data/raw_csv"))

    if not password:
        raise ValueError("PGPASSWORD is missing. Put it in your environment or .env.")

    return Settings(host, port, database, user, password, raw_csv_dir)
