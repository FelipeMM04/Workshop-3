import json
import os
import joblib
import pandas as pd
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from event_contract import validate_event_contract

# Configuraciones Kafka y DB
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'happiness-events'
CONSUMER_GROUP = 'happiness-ml-team'
MODEL_PATH = '../models/model.pkl' if os.path.exists('../models/model.pkl') else 'models/model.pkl'

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "happiness_db"
DB_USER = "admin"
DB_PASS = "adminpassword"


def get_db_connection():
    """Crea una conexión con PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def load_ml_model():
    """Carga el pipeline del modelo serializado (model.pkl)."""
    print(f"📦 Cargando modelo ML desde: {MODEL_PATH}")
    try:
        model = joblib.load(MODEL_PATH)
        print("✅ Modelo cargado correctamente en memoria.")
        return model
    except Exception as e:
        print(f"❌ Error crítico al cargar el modelo: {e}")
        exit(1)


def get_or_create_country(cursor, country_name):
    """Busca o inserta un país en dim_country y retorna su country_id."""
    cursor.execute("SELECT country_id FROM dim_country WHERE country_name = %s;", (country_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    
    cursor.execute(
        "INSERT INTO dim_country (country_name) VALUES (%s) RETURNING country_id;",
        (country_name,)
    )
    return cursor.fetchone()[0]


def get_or_create_date(cursor, event_time_str):
    """Parsea event_time, busca o inserta en dim_date y retorna date_id (YYYYMMDD)."""
    dt = datetime.fromisoformat(event_time_str)
    date_id = int(dt.strftime("%Y%m%d"))
    full_date = dt.date()

    cursor.execute("SELECT date_id FROM dim_date WHERE date_id = %s;", (date_id,))
    row = cursor.fetchone()
    if row:
        return date_id

    cursor.execute(
        """
        INSERT INTO dim_date (date_id, full_date, year, month, day, day_of_week)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (date_id, full_date, dt.year, dt.month, dt.day, dt.weekday())
    )
    return date_id


def save_raw_event(conn, event_id, partition, offset, payload_str, status, error_msg=None):
    """Guarda el evento RAW y retorna su raw_id autogenerado."""
    query = """
    INSERT INTO raw_happiness_events 
    (event_id, topic_name, kafka_partition, kafka_offset, payload, processing_status, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING raw_id;
    """
    cursor = conn.cursor()
    cursor.execute(query, (event_id, TOPIC_NAME, partition, offset, payload_str, status, error_msg))
    raw_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return raw_id


def save_prediction(conn, raw_event_id, raw_id, country_id, date_id, actual_score, pred_score, error, event_time):
    """Guarda la predicción en fact_predictions vinculando las llaves dimensionales."""
    query = """
    INSERT INTO fact_predictions 
    (raw_event_id, raw_id, country_id, date_id, actual_score, predicted_score, prediction_error, prediction_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor = conn.cursor()
    cursor.execute(query, (raw_event_id, raw_id, country_id, date_id, actual_score, pred_score, error, event_time))
    conn.commit()
    cursor.close()


def run_consumer():
    model = load_ml_model()
    conn = get_db_connection()

    print(f"🚀 Conectando Consumer a Kafka (Topic: '{TOPIC_NAME}', Grupo: '{CONSUMER_GROUP}')...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    print("=== CONSUMER ESCUCHANDO EVENTOS CON MODELO DIMENSIONAL (STEP 7) ===")
    print("Presiona Ctrl + C para detener.\n")

    try:
        for message in consumer:
            payload_str = message.value.decode('utf-8')
            event_data = None
            event_id = None

            try:
                event_data = json.loads(payload_str)
                event_id = event_data.get("event_id", "UNKNOWN")
            except Exception as json_err:
                save_raw_event(conn, "UNKNOWN", message.partition, message.offset, payload_str, "INVALID_SCHEMA", str(json_err))
                print(f"⚠️ [RECHAZADO] Mensaje en offset {message.offset} no es JSON valido.")
                continue

            # Validar contrato
            is_valid, validation_status = validate_event_contract(event_data)

            if not is_valid:
                save_raw_event(conn, event_id, message.partition, message.offset, payload_str, validation_status, validation_status)
                print(f"⚠️ [RECHAZADO] Evento {event_id} -> Estado: {validation_status}")
                continue

            # 1. Guardar en RAW y obtener raw_id
            raw_id = save_raw_event(conn, event_id, message.partition, message.offset, payload_str, "VALID", None)

            # 2. Inferencia ML
            features_df = pd.DataFrame([{
                'gdp': float(event_data['gdp']),
                'family': float(event_data['family']),
                'health': float(event_data['health']),
                'freedom': float(event_data['freedom']),
                'generosity': float(event_data['generosity']),
                'corruption': float(event_data['corruption'])
            }])

            try:
                predicted_score = float(model.predict(features_df)[0])
                actual_score = float(event_data['actual_happiness_score'])
                abs_error = abs(actual_score - predicted_score)

                # 3. Poblar dimensiones y obtener IDs
                cursor = conn.cursor()
                country_id = get_or_create_country(cursor, str(event_data['country']))
                date_id = get_or_create_date(cursor, event_data['event_time'])
                conn.commit()
                cursor.close()

                # 4. Guardar en fact_predictions
                save_prediction(
                    conn,
                    event_id,
                    raw_id,
                    country_id,
                    date_id,
                    actual_score,
                    predicted_score,
                    abs_error,
                    event_data['event_time']
                )

                print(
                    f"🔵 [{event_data['event_time']}] | "
                    f"ID: {event_id} | "
                    f"País: {event_data['country']:<15} (ID: {country_id}) | "
                    f"Real: {actual_score:.2f} | "
                    f"Predicho: {predicted_score:.2f} | "
                    f"Error: {abs_error:.4f}"
                )

            except Exception as pred_err:
                print(f"❌ Error de predicción en evento {event_id}: {pred_err}")

    except KeyboardInterrupt:
        print("\n⏹️ Consumer detenido por el usuario.")
    finally:
        conn.close()
        consumer.close()


if __name__ == '__main__':
    run_consumer()