import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class DatabaseManagerGold:
    def __init__(self):
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", "sinistros_prf")
        self.db_user = os.getenv("DB_USER", "admin")
        self.db_password = os.getenv("DB_PASSWORD", "admin123")
        self.schema_default = "gold"
        self._engine = None

    @property
    def engine(self):
        """Property para acesso ao engine SQLAlchemy"""
        return self.get_engine()

    def get_engine(self):
        if self._engine is None:
            conn_str = (
                f"postgresql+psycopg2://{self.db_user}:{self.db_password}@"
                f"{self.db_host}:{self.db_port}/{self.db_name}"
            )
            self._engine = create_engine(conn_str)
        return self._engine

    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            with self.get_engine().connect() as conn:
                df = pd.read_sql(text(query), conn)
            return df
        except SQLAlchemyError as e:
            print(f"[GOLD DB] Erro ao executar query: {e}")
            raise

    def execute_sql(self, sql: str) -> None:
        try:
            with self.get_engine().connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except SQLAlchemyError as e:
            print(f"[GOLD DB] Erro ao executar SQL: {e}")
            raise

    def test_connection(self) -> bool:
        try:
            with self.get_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            print("[GOLD DB] ✅ Conexão com o banco bem-sucedida!")
            return True
        except Exception as e:
            print(f"[GOLD DB] ❌ Falha ao conectar: {e}")
            return False

    def ensure_schema(self, schema_name: str = None):
        schema = schema_name or self.schema_default
        try:
            with self.get_engine().connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                conn.commit()
            print(f"[GOLD DB] Schema '{schema}' verificado/criado com sucesso.")
        except SQLAlchemyError as e:
            print(f"[GOLD DB] Erro ao criar/verificar schema {schema}: {e}")


db_manager = DatabaseManagerGold()
