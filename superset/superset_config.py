SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/superset"


# 🟢 habilita templates / features
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True
}

# 🟢 habilita tema via UI
ENABLE_UI_THEME_ADMINISTRATION = True
