import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ['id', 'device_id', 'collection', 'time_stamp'] # after run test producer to see log

def is_valid(record):
    return all(field in record for field in REQUIRED_FIELDS)

def main():
    consumer = KafkaConsumer(
        config.DEST_KAFKA_TOPIC,
        bootstrap_servers=config.DEST_KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # manual commit for resume
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=config.DEST_KAFKA_USERNAME,
        sasl_plain_password=config.DEST_KAFKA_PASSWORD,
        max_poll_interval_ms = 300000,      # 5 mins
        session_timeout_ms = 30000,         # 30 sec
        heartbeat_interval_ms = 10000       # 10 sec
    )

    mongo_client = MongoClient(config.MONGO_URI)
    collection = mongo_client[config.MONGO_DB][config.MONGO_COLLECTION]

    logger.info(f'Reading from local Kafka: {config.DEST_KAFKA_TOPIC}')
    logger.info(f'Saving to MongoDB: {config.MONGO_DB}.{config.MONGO_COLLECTION}')

    try:
        for message in consumer:
            record = message.value

            if not is_valid(record):
                logger.warning(f'Invalid message, skipping: {record}')
                consumer.commit()
                continue

            record['consumed_at'] = datetime.utcnow().isoformat()
            record['partition'] = message.partition
            record['offset'] = message.offset

            try:
                # Upsert → exactly-once at storage level
                # after running test producer to see log
                collection.update_one(
                    {'id': record['id']},  # unique per event
                    {'$set': record},
                    upsert=True
                )
                # Commit offset only after successful save
                consumer.commit()
                logger.info(f'Saved | partition={message.partition} | offset={message.offset}')
            except PyMongoError as e:
                logger.error(f'MongoDB error: {e} — skipping commit, will retry on restart')

    except KeyboardInterrupt:
        logger.info('Stopping consumer...')
    finally:
        consumer.close()
        mongo_client.close()

if __name__ == '__main__':
    main()