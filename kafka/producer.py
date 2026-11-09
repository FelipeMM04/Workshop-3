import json
import time
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Configuración de variables globales del Producer
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'happiness-events'
DATA_PATH = 'data/processed/cleaned_happiness.csv'
DELAY_SECONDS = 0.5  # Pausa entre eventos para simular flujo en tiempo real


def create_kafka_topic():
    """
    Crea el topic 'happiness-events' con 3 particiones y factor de replicación 1
    cumpliendo estrictamente las especificaciones del entorno local.
    """
    print("🔄 Conectando con Kafka Admin Client...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='admin_producer'
        )

        # Verificar si el topic ya existe
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME in existing_topics:
            print(f"ℹ️ El topic '{TOPIC_NAME}' ya existe en el cluster.")
        else:
            topic_list = [
                NewTopic(
                    name=TOPIC_NAME,
                    num_partitions=3,
                    replication_factor=1
                )
            ]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"✅ Topic '{TOPIC_NAME}' creado exitosamente con 3 particiones y replicación 1.")
            
        admin_client.close()
    except Exception as e:
        print(f"⚠️ Aviso al gestionar el topic en Kafka: {e}")


def json_serializer(data):
    """Serializa los mensajes a formato JSON utf-8."""
    return json.dumps(data).encode('utf-8')


def run_producer():
    """
    Lee el archivo CSV procesado registro por registro y los transmite
    a Apache Kafka usando el país como 'Event Key'.
    """
    # 1. Asegurar la existencia del Topic
    create_kafka_topic()

    # 2. Inicializar KafkaProducer
    print("🚀 Inicializando Kafka Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda k: str(k).encode('utf-8'),
        value_serializer=json_serializer,
        acks='all'
    )

    # 3. Cargar el dataset procesado
    print(f"📖 Leyendo datos desde: {DATA_PATH}")
    try:
        df = pd.read_csv(DATA_PATH)
    except FileNotFoundError:
        # Intento fallback si la ejecución ocurre desde dentro de la carpeta kafka/
        df = pd.read_csv('../' + DATA_PATH)

    total_records = len(df)
    print(f"📦 Total de eventos a transmitir: {total_records}\n")
    print("=== INICIANDO TRANSMISIÓN STREAMING EN TIEMPO REAL ===")

    # 4. Iterar registro por registro
    for idx, row in df.iterrows():
        # Generar Event ID (E001, E002, ...)
        event_id = f"E{idx + 1:03d}"
        
        # Estructura del evento JSON requerida
        event_payload = {
            "event_id": event_id,
            "country": str(row['country']),
            "year": int(row['year']),
            "gdp": float(row['gdp']),
            "family": float(row['family']),
            "health": float(row['health']),
            "freedom": float(row['freedom']),
            "generosity": float(row['generosity']),
            "corruption": float(row['corruption']),
            "actual_happiness_score": float(row['actual_happiness_score']),
            "event_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }

        # Event Key: 'country'
        event_key = str(row['country'])

        # Enviar evento a Kafka
        future = producer.send(
            topic=TOPIC_NAME,
            key=event_key,
            value=event_payload
        )
        
        # Obtener metadata de la entrega
        record_metadata = future.get(timeout=10)

        print(
            f"🟢 [{event_payload['event_time']}] Enviado: {event_id} | "
            f"País: {event_key:<15} | "
            f"Partición: {record_metadata.partition} | "
            f"Offset: {record_metadata.offset}"
        )

        time.sleep(DELAY_SECONDS)

    producer.flush()
    producer.close()
    print("\n✅ Transmisión finalizada exitosamente. Todos los eventos fueron publicados.")


if __name__ == '__main__':
    run_producer()