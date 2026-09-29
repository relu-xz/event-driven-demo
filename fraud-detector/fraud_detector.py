import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("fraud-detector")

# Variables de entorno con valores por defecto
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "tx-received")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "tx-evaluated")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detection-service")

# Control de ciclo de vida
running = True


def handle_shutdown(sig, frame):
    global running
    logger.info("Recibida senal de terminacion (%s). Cerrando servicio ordenadamente...", sig)
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def delivery_callback(err, msg):
    """Callback invocado por el productor al recibir el ACK del broker."""
    if err:
        logger.error("Fallo al entregar mensaje en '%s' [%s]: %s", 
                     msg.topic(), msg.partition(), err)
    else:
        logger.info("Mensaje evaluado publicado con exito en '%s' [Particion: %s, Offset: %s]",
                    msg.topic(), msg.partition(), msg.offset())


def evaluate_transaction(tx: dict) -> dict:
    """Aplica reglas de deteccion de fraude."""
    amount = float(tx.get("amount", 0.0))
    target_account = str(tx.get("targetAccount", ""))

    risk_score = 0.05
    status = "APPROVED"
    reason = "PASSED_INITIAL_CHECKS"

    # Regla 1: Montos mayores a $5,000 despiertan alerta
    if amount >= 5000.0:
        risk_score = 0.92
        status = "REJECTED"
        reason = "AMOUNT_EXCEEDS_THRESHOLD"

    # Regla 2: Cuentas fraudulentas simuladas
    elif target_account.startswith("SUSPECT") or target_account.endswith("9999"):
        risk_score = 0.98
        status = "REJECTED"
        reason = "SUSPICIOUS_TARGET_ACCOUNT"

    # Construccion del evento evaluado
    evaluated_tx = tx.copy()
    evaluated_tx["status"] = status
    evaluated_tx["riskScore"] = risk_score
    evaluated_tx["rejectionReason"] = reason if status == "REJECTED" else None
    evaluated_tx["evaluatedAt"] = datetime.now(timezone.utc).isoformat()
    evaluated_tx["evaluatedBy"] = "python-fraud-engine-v1"

    return evaluated_tx


def main():
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False  # Control manual del commit tras procesar
    }

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "python-fraud-producer",
        "acks": "all"
    }

    logger.info("Iniciando servicio de deteccion de fraude...")
    logger.info("Conectando a Kafka: %s | Consumiendo de: %s | Produciendo a: %s",
                BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC)

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)

    consumer.subscribe([INPUT_TOPIC])

    try:
        while running:
            # Poll con timeout de 1 segundo para no bloquear senales del sistema
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Fin de particion alcanzado: %s [%s]", msg.topic(), msg.partition())
                elif msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
                    logger.warning("Topico '%s' aun no disponible en el broker. Esperando...", INPUT_TOPIC)
                    time.sleep(2)
                else:
                    raise KafkaException(msg.error())

                continue

            # Procesamiento del payload
            try:
                raw_payload = msg.value().decode("utf-8")
                tx_data = json.loads(raw_payload)
                tx_id = tx_data.get("id", "UNKNOWN")

                logger.info("Procesando transaccion entrante: ID=%s, Monto=%s %s",
                            tx_id, tx_data.get("amount"), tx_data.get("currency"))

                # Evaluacion
                result = evaluate_transaction(tx_data)
                logger.info("Transaccion ID=%s resultado: %s (RiskScore=%.2f)",
                            tx_id, result["status"], result["riskScore"])

                # Publicar resultado al topico de salida
                serialized_result = json.dumps(result).encode("utf-8")
                producer.produce(
                    topic=OUTPUT_TOPIC,
                    key=str(tx_id).encode("utf-8"),
                    value=serialized_result,
                    on_delivery=delivery_callback
                )

                # Servir eventos de entrega pendientes y commitear offset
                producer.poll(0)
                consumer.commit(message=msg, asynchronous=False)

            except json.JSONDecodeError as jde:
                logger.error("Error al decodificar JSON del mensaje offset %s: %s", msg.offset(), jde)
                consumer.commit(message=msg, asynchronous=False)
            except Exception as e:
                logger.exception("Error inesperado procesando mensaje: %s", e)

    finally:
        logger.info("Liberando recursos de Kafka...")
        producer.flush(timeout=5.0)
        consumer.close()
        logger.info("Servicio detenido correctamente.")


if __name__ == "__main__":
    main()
