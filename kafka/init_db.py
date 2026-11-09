import psycopg2
from psycopg2 import OperationalError

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "happiness_db"
DB_USER = "admin"
DB_PASS = "adminpassword"


def init_database():
    """
    Reinicia y crea la estructura dimensional exacta exigida en el Step 7:
    - raw_happiness_events (Capa Staging)
    - dim_country (Dimensión País)
    - dim_date (Dimensión Fecha)
    - fact_predictions (Tabla de Hechos Analítica con FKs)
    """
    print("🔄 Reiniciando y aplicando el diseño dimensional del Step 7 en PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()

        # 1. Eliminar tablas antiguas para asegurar la migración de esquema
        cursor.execute("DROP TABLE IF EXISTS fact_predictions CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS raw_happiness_events CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_country CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_date CASCADE;")

        # 2. Crear Tabla Raw / Auditoría (Staging)
        create_raw_table_query = """
        CREATE TABLE raw_happiness_events (
            raw_id SERIAL PRIMARY KEY,
            event_id VARCHAR(50),
            topic_name VARCHAR(100),
            kafka_partition INT,
            kafka_offset BIGINT,
            payload JSONB NOT NULL,
            processing_status VARCHAR(50) NOT NULL,
            error_message TEXT,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # 3. Crear Dimensión País
        create_dim_country_query = """
        CREATE TABLE dim_country (
            country_id SERIAL PRIMARY KEY,
            country_name VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # 4. Crear Dimensión Fecha
        create_dim_date_query = """
        CREATE TABLE dim_date (
            date_id INT PRIMARY KEY, -- Formato YYYYMMDD
            full_date DATE NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            day_of_week INT NOT NULL
        );
        """

        # 5. Crear Tabla de Hechos (Fact Table con FKs exactas del Step 7)
        create_fact_predictions_query = """
        CREATE TABLE fact_predictions (
            prediction_id SERIAL PRIMARY KEY,
            raw_event_id VARCHAR(50) NOT NULL,
            raw_id INT REFERENCES raw_happiness_events(raw_id),
            country_id INT REFERENCES dim_country(country_id),
            date_id INT REFERENCES dim_date(date_id),
            actual_score FLOAT NOT NULL,
            predicted_score FLOAT NOT NULL,
            prediction_error FLOAT NOT NULL,
            prediction_timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        cursor.execute(create_raw_table_query)
        cursor.execute(create_dim_country_query)
        cursor.execute(create_dim_date_query)
        cursor.execute(create_fact_predictions_query)

        conn.commit()
        print("✅ Esquema dimensional del Step 7 creado correctamente con todas las llaves relacionales.")

        cursor.close()
        conn.close()

    except OperationalError as e:
        print(f"❌ Error al conectar con PostgreSQL: {e}")


if __name__ == "__main__":
    init_database()