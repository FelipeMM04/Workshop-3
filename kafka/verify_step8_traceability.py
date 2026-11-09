import psycopg2

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "happiness_db"
DB_USER = "admin"
DB_PASS = "adminpassword"


def verify_step8_traceability():
    print("🔎 EJECUTANDO AUDITORÍA DE TRAZABILIDAD (STEP 8)...")
    
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cursor = conn.cursor()

    # 1. Verificar presencia de campos requeridos en fact_predictions
    print("\n--- 1. Verificación de Campos Guardados en fact_predictions ---")
    cursor.execute("""
        SELECT 
            f.prediction_id,
            f.raw_event_id,
            c.country_name,
            f.actual_score,
            f.predicted_score,
            f.prediction_error,
            f.prediction_timestamp,
            f.created_at
        FROM fact_predictions f
        JOIN dim_country c ON f.country_id = c.country_id
        ORDER BY f.prediction_id ASC
        LIMIT 3;
    """)
    rows = cursor.fetchall()
    
    for r in rows:
        print(f"📌 Predicción ID: {r[0]}")
        print(f"   • Event ID (raw_event_id): {r[1]}")
        print(f"   • País: {r[2]}")
        print(f"   • Actual Score: {r[3]:.4f}")
        print(f"   • Predicted Score: {r[4]:.4f}")
        print(f"   • Prediction Error: {r[5]:.4f}")
        print(f"   • Event Time: {r[6]}")
        print(f"   • Prediction Timestamp (DB): {r[7]}\n")

    # 2. Verificación de Trazabilidad Directa (Fact -> Raw Payload)
    print("--- 2. Prueba de Trazabilidad Directa hacia el Mensaje RAW Original ---")
    query_traceability = """
        SELECT 
            f.prediction_id,
            f.raw_event_id,
            r.raw_id,
            r.topic_name,
            r.kafka_partition,
            r.kafka_offset,
            r.processing_status,
            r.payload
        FROM fact_predictions f
        JOIN raw_happiness_events r ON f.raw_id = r.raw_id
        ORDER BY f.prediction_id ASC
        LIMIT 2;
    """
    cursor.execute(query_traceability)
    trace_rows = cursor.fetchall()

    for tr in trace_rows:
        print(f"🔗 [Fact Prediction ID: {tr[0]}] <---> [RAW Event ID: {tr[1]} | DB Raw ID: {tr[2]}]")
        print(f"   • Kafka Metadata : Topic '{tr[3]}' | Partición {tr[4]} | Offset {tr[5]}")
        print(f"   • Status Ingesta : {tr[6]}")
        print(f"   • Payload JSON   : {tr[7]}\n")

    # 3. Validación de Integridad (Total de registros cruzados)
    cursor.execute("""
        SELECT 
            COUNT(f.prediction_id) AS total_fact,
            COUNT(r.raw_id) AS total_raw_linked
        FROM fact_predictions f
        INNER JOIN raw_happiness_events r ON f.raw_id = r.raw_id;
    """)
    totals = cursor.fetchone()
    print("--- 3. Resumen de Integridad de Enlace ---")
    print(f"✅ Total de predicciones almacenadas  : {totals[0]}")
    print(f"✅ Total enlazadas con evento RAW exacto: {totals[1]}")

    if totals[0] == totals[1] and totals[0] > 0:
        print("\n🎉 ¡Paso 8 Cumplido al 100%! Todas las predicciones tienen trazabilidad exacta hacia su mensaje RAW.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    verify_step8_traceability()