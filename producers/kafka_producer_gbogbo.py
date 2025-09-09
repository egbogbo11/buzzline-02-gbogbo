"""
kafka_producer_gbogbo.py

Produce travel/flight alert messages and send them to a Kafka topic.
Small changes: default topic + flight-themed sample messages (text + JSON).
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os
import sys
import time
from typing import Iterable, Union

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger


#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "flight_alerts")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Message Generator
#####################################


def _travel_messages() -> Iterable[Union[str, dict]]:
    """
    A small rotating set of flight-themed messages.
    Mix of plaintext and JSON (as dict) so the consumer can do simple analytics.
    """
    return [
        # Plain text that should match your ALERT_PATTERNS (DELAY, CANCELLED, etc.)
        "GATE_CHANGE: DL789 moved to A3",
        "BOARDING: AA123 now boarding Group 1 at C17",
        "CANCELLED: UA456 due to weather",
        "PRICE_DROP: NYC->LAX roundtrip fell by $120",

        # JSON samples for numeric/status-based alerts your consumer looks for
        {"status": "delayed", "flight": "AA123", "airline": "American", "delay_minutes": 75},
        {"status": "on_time", "flight": "DL789", "airline": "Delta", "gate": "A3"},
        {"status": "cancelled", "flight": "UA456", "airline": "United", "gate": "B12"},
        {"status": "on_time", "flight": "B6222", "airline": "JetBlue", "price_drop": 120.0},
    ]


def generate_messages(producer, topic: str, interval_secs: int) -> None:
    """
    Generate a stream of flight alert messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.

    """
    try:
        while True:
            for item in _travel_messages():
                if isinstance(item, dict):
                    message = json.dumps(item)
                else:
                    message = str(item)

                logger.info(f"Generated travel alert: {message}")
                producer.send(topic, value=message)
                logger.info(f"Sent message to topic '{topic}': {message}")
                time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        try:
            producer.flush()
        except Exception:
            pass
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams travel/flight alert messages to the Kafka topic.
    """
    logger.info("START producer.")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
