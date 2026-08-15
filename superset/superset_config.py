import os

DB_PASSWORD = os.environ.get("DB_PASSWORD")
RDS_HOST = os.environ.get("RDS_HOST")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://airflow:{DB_PASSWORD}@{RDS_HOST}:5432/superset"

# 🟢 habilita templates / features
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True
}

# 🟢 habilita tema via UI
ENABLE_UI_THEME_ADMINISTRATION = True
