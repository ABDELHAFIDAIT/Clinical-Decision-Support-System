from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import logging
import os


try:
    from dotenv import load_dotenv
    load_dotenv() 
except ImportError:
    logging.warning("Librairie python-dotenv non trouvée!")



DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "cdss_knowledge_base"),
    "user": os.getenv("POSTGRES_USER", "admin_cdss"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "postgres",
    "port": os.getenv("POSTGRES_PORT", "5432")
}


def init_database_schema():
    conn = None
    
    if not DB_CONFIG["password"]:
        raise ValueError("ERREUR: Le mot de passe POSTGRES_PASSWORD n'est pas défini dans l'environnement.")

    try:
        logging.info(f"Connexion à la DB {DB_CONFIG['dbname']} sur {DB_CONFIG['host']}...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        schema_sql = """
        DROP TABLE IF EXISTS audit_logs CASCADE;
        DROP TABLE IF EXISTS contraindications CASCADE;
        DROP TABLE IF EXISTS interactions CASCADE;
        DROP TABLE IF EXISTS drugs CASCADE;

        CREATE TABLE drugs (
            rxcui VARCHAR(20) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            strength_value NUMERIC(10, 2),
            strength_unit VARCHAR(20),
            max_daily_dose NUMERIC(10, 2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_drugs_name ON drugs(name);

        CREATE TABLE interactions (
            id SERIAL PRIMARY KEY,
            drug_1_rxcui VARCHAR(20) NOT NULL REFERENCES drugs(rxcui),
            drug_2_rxcui VARCHAR(20) NOT NULL REFERENCES drugs(rxcui),
            severity VARCHAR(20) NOT NULL,
            description TEXT,
            mechanism VARCHAR(255),
            CONSTRAINT unique_interaction UNIQUE (drug_1_rxcui, drug_2_rxcui)
        );
        CREATE INDEX idx_interactions_check ON interactions(drug_1_rxcui, drug_2_rxcui);

        CREATE TABLE contraindications (
            id SERIAL PRIMARY KEY,
            drug_rxcui VARCHAR(20) NOT NULL REFERENCES drugs(rxcui),
            condition_code VARCHAR(50) NOT NULL,
            threshold_value NUMERIC(10, 2),
            severity VARCHAR(20) DEFAULT 'HIGH'
        );
        """

        cur.execute(schema_sql)
        
        seed_sql = """
        INSERT INTO drugs (rxcui, name, strength_value, strength_unit, max_daily_dose) VALUES 
        ('12345', 'Doliprane 1000mg', 1000, 'mg', 4000),
        ('67890', 'Sintrom 4mg', 4, 'mg', 10);
        
        INSERT INTO interactions (drug_1_rxcui, drug_2_rxcui, severity, description) VALUES
        ('12345', '67890', 'MODERATE', 'Risque accru de saignement (Interaction simulée).');
        """
        cur.execute(seed_sql)
        
        conn.commit()
        cur.close()
        logging.info("Succès : Schéma initialisé avec les variables d'environnement.")

    except Exception as e:
        logging.error(f"Erreur : {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


default_args = {
    'owner': 'abdelhafid',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 25),
    'retries': 1,
}

with DAG(
    '01_init_schema_cdss_secure',
    default_args=default_args,
    description='Initialise les tables SQL via Env Vars',
    schedule_interval='@once',
    catchup=False,
    tags=['cdss', 'setup']
) as dag:

    init_db_task = PythonOperator(
        task_id='create_tables_sql_secure',
        python_callable=init_database_schema
    )

    init_db_task