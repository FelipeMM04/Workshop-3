import psycopg2

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "happiness_db"
DB_USER = "admin"
DB_PASS = "adminpassword"


def check_analytics_data():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cursor = conn.cursor()

    print("=== ESTADO DE LAS TABLAS EN POSTGRESQL (STEP 7) ===")

    # 1. Total en Raw Table
    cursor.execute("SELECT COUNT(*) FROM raw_happiness_events;")
    raw_count = cursor.fetchone()[0]
    print(f"📦 raw_happiness_events : {raw_count} registros")

    # 2. Total en Dimensión País
    cursor.execute("SELECT COUNT(*) FROM dim_country;")
    dim_country_count = cursor.fetchone()[0]
    print(f"🌍 dim_country           : {dim_country_count} países insertados")

    # 3. Total en Dimensión Fecha
    cursor.execute("SELECT COUNT(*) FROM dim_date;")
    dim_date_count = cursor.fetchone()[0]
    print(f"📅 dim_date              : {dim_date_count} fechas insertadas")

    # 4. Total en Tabla de Hechos
    cursor.execute("SELECT COUNT(*) FROM fact_predictions;")
    fact_count = cursor.fetchone()[0]
    print(f"📊 fact_predictions      : {fact_count} predicciones almacenadas")

    # Muestra de join relacional
    if fact_count > 0:
        print("\n=== MUESTRA DE INNER JOIN ANALÍTICO (HECHOS + DIMENSIONES) ===")
        sample_query = """
        SELECT 
            f.prediction_id,
            f.raw_event_id,
            c.country_name,
            d.full_date,
            f.actual_score,
            f.predicted_score,
            f.prediction_error
        FROM fact_predictions f
        JOIN dim_country c ON f.country_id = c.country_id
        JOIN dim_date d ON f.date_id = d.date_id
        LIMIT 5;
        """
        cursor.execute(sample_query)
        rows = cursor.fetchall()
        for r in rows:
            print(
                f"ID: {r[0]} | Evento: {r[1]} | País: {r[2]:<15} | Fecha: {r[3]} | "
                f"Real: {r[4]:.2f} | Predicho: {r[5]:.2f} | Error: {r[6]:.4f}"
            )

    cursor.close()
    conn.close()


if __name__ == "__main__":
    check_analytics_data()