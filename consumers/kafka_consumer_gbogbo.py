"""
kafka_consumer_elom_travel.py

Consume messages from a Kafka topic and process them.
Small changes: travel/flight topic defaults + simple real-time alerts.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os
import re
from typing import List

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "flight_alerts")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "travel_consumer_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_alert_patterns() -> List[str]:
    """
    Patterns to alert on (pipe-separated in ALERT_PATTERNS).
    Example: DELAY|CANCELLED|GATE_CHANGE|BOARDING|PRICE_DROP
    """
    default_patterns = "DELAY|CANCELLED|GATE_CHANGE|BOARDING|PRICE_DROP"
    raw = os.getenv("ALERT_PATTERNS", default_patterns)
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    logger.info(f"Alert patterns loaded: {parts}")
    return parts


def get_thresholds() -> tuple[int, float]:
    """
    Load numeric thresholds:
      - MIN_DELAY_MINUTES: trigger alert when delay_minutes >= this value (default 60)
      - PRICE_DROP_THRESHOLD: trigger alert for price_drop >= this value (default 50.0)
    """
    try:
        min_delay = int(os.getenv("MIN_DELAY_MINUTES", "60"))
    except ValueError:
        min_delay = 60

    try:
        price_drop = float(os.getenv("PRICE_DROP_THRESHOLD", "50.0"))
    except ValueError:
        price_drop = 50.0

    logger.info(f"Thresholds: MIN_DELAY_MINUTES={min_delay}, PRICE_DROP_THRESHOLD={price_drop}")
    return min_delay, price_drop


#####################################
# Define a function to process a single message
#####################################


def process_message(message: str, alert_regex: re.Pattern, min_delay: int, price_drop_threshold: float) -> None:
    """
    Process a single message.

    - Logs message
    - If JSON, inspects common flight keys and raises alerts on:
        * delay_minutes >= MIN_DELAY_MINUTES
        * status like CANCELLED
        * price_drop >= PRICE_DROP_THRESHOLD
    - Always checks text for ALERT_PATTERNS
    """
    logger.info(f"Processing message: {message}")

    payload = None
    try:
        payload = json.loads(message)
    except Exception:
        payload = None

    if isinstance(payload, dict):
        # Typical flight fields (optional, best-effort)
        status = str(payload.get("status", "")).upper()
        flight = payload.get("flight")
        airline = payload.get("airline")
        gate = payload.get("gate")
        delay_minutes = payload.get("delay_minutes")
        price_drop = payload.get("price_drop")

        # Numeric thresholds
        if isinstance(delay_minutes, (int, float)) and delay_minutes >= min_delay:
            logger.warning(
                f"ALERT: Significant delay detected (delay_minutes={delay_minutes} >= {min_delay}). Payload: {payload}"
            )

        if isinstance(price_drop, (int, float)) and price_drop >= price_drop_threshold:
            logger.warning(
                f"ALERT: Price drop detected (price_drop={price_drop} >= {price_drop_threshold}). Payload: {payload}"
            )

        # Status-based alerts (e.g., CANCELLED)
        if status and alert_regex.search(status):
            logger.warning(f"ALERT: Status pattern match '{status}'. Payload: {payload}")

        logger.debug(
            f"JSON fields: status={status!r}, flight={flight!r}, airline={airline!r}, gate={gate!r}, "
            f"delay_minutes={delay_minutes!r}, price_drop={price_drop!r}"
        )

    # Text-based pattern match (works for plaintext or JSON string)
    if alert_regex.search(message.upper()):
        logger.warning(f"ALERT: Pattern match in text message: {message}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    patterns = get_alert_patterns()
    min_delay, price_drop_threshold = get_thresholds()
    alert_regex = re.compile("|".join(map(re.escape, patterns)), flags=re.IGNORECASE)

    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            try:
                message_str = (
                    message.value.decode("utf-8", errors="ignore")
                    if isinstance(message.value, (bytes, bytearray))
                    else str(message.value)
                )
            except Exception as decode_err:
                logger.error(f"Failed to decode message.value: {decode_err}")
                continue

            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, alert_regex, min_delay, price_drop_threshold)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")
    

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
