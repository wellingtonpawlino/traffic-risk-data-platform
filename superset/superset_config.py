import os

DB_PASSWORD = os.environ.get("DB_PASSWORD")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://airflow:{DB_PASSWORD}@traffic-risk-postgres.cgf4cckuyhc9.us-east-1.rds.amazonaws.com:5432/superset"

# 🟢 habilita templates / features
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True
}

# 🟢 habilita tema via UI
ENABLE_UI_THEME_ADMINISTRATION = True
