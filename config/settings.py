from pydantic_settings import BaseSettings


class DatabricksSettings(BaseSettings):
    host: str = ""
    token: str = ""
    catalog: str = "ecommerce"
    schema_bronze: str = "bronze"
    schema_silver: str = "silver"
    schema_gold: str = "gold"

    model_config = {"env_prefix": "DATABRICKS_"}


class PostgresSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    db: str = "ecommerce_source"
    user: str = "cdc_reader"
    password: str = ""

    model_config = {"env_prefix": "POSTGRES_"}

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class Settings(BaseSettings):
    databricks: DatabricksSettings = DatabricksSettings()
    postgres: PostgresSettings = PostgresSettings()
    delta_lake_path: str = "/tmp/delta-lake/ecommerce"
    product_api_url: str = "https://fakestoreapi.com/products"
    quality_fail_threshold: float = 0.95

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
