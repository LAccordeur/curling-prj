from sqlalchemy import create_engine
from config import DB_MAX_OVERFLOW, DB_POOL_SIZE, DB_POOL_TIMEOUT, DB_URL

engine = create_engine(
    DB_URL,
    future=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT,
)
