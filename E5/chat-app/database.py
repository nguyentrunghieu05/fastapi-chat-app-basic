from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# MySQL Configuration
DB_USER = "root"
DB_PASSWORD = "123456789"  # Thay đổi mật khẩu của bạn tại đây
DB_HOST = "127.0.0.1"
DB_PORT = "3306"
DB_NAME = "blogdb"

SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()