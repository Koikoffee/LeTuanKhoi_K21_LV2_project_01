import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def on_send_success(record_metadata):
    logger.info(f'Forwarded to local Kafka | partition={record_metadata.partition} | offset={record_metadata.offset}')

def on_send_error(excp):
    logger.error(f'Failed to forward message: {excp}')

def main():
    # Read from external Kafka source
    source_consumer = KafkaConsumer(
        config.SOURCE_KAFKA_TOPIC,
        bootstrap_servers=config.SOURCE_KAFKA_BOOTSTRAP_SERVERS,
        group_id='source-reader-group',
        auto_offset_reset='latest',  # stream mode - read live data only
        enable_auto_commit=False,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=config.SOURCE_KAFKA_USERNAME,
        sasl_plain_password=config.SOURCE_KAFKA_PASSWORD
    )

    # Produce to local Kafka
    # enable_idempotence=True → exactly-once at producer level
    dest_producer = KafkaProducer(
        bootstrap_servers=config.DEST_KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=config.DEST_KAFKA_USERNAME,
        sasl_plain_password=config.DEST_KAFKA_PASSWORD,
        enable_idempotence=True,  # exactly-once guarantee
        retries=5
    )

    logger.info(f'Reading from external Kafka: {config.SOURCE_KAFKA_TOPIC}')
    logger.info(f'Forwarding to local Kafka: {config.DEST_KAFKA_TOPIC}')

    try:
        for message in source_consumer:
            # Log raw data
            # logger.info(f'Raw message: {message.value}') # cmt after checking REQUIRED_FIELDS from consumer.py
            dest_producer.send(
                config.DEST_KAFKA_TOPIC,
                value=message.value,
                key=message.key
            ).add_callback(on_send_success).add_errback(on_send_error)

            # Commit source offset only after forwarding successfully
            source_consumer.commit()

    except KeyboardInterrupt:
        logger.info('Stopping producer...')
    finally:
        dest_producer.flush()
        dest_producer.close()
        source_consumer.close()

if __name__ == '__main__':
    main()