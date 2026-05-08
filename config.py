import os
from dotenv import load_dotenv

load_dotenv()

# Source Kafka (external)
SOURCE_KAFKA_BOOTSTRAP_SERVERS = os.getenv('SOURCE_KAFKA_BOOTSTRAP_SERVERS')
SOURCE_KAFKA_TOPIC = os.getenv('SOURCE_KAFKA_TOPIC', 'product_view')
SOURCE_KAFKA_USERNAME = os.getenv('SOURCE_KAFKA_USERNAME', 'kafka')
SOURCE_KAFKA_PASSWORD = os.getenv('SOURCE_KAFKA_PASSWORD')

# Destination Kafka (local)
DEST_KAFKA_BOOTSTRAP_SERVERS = os.getenv('DEST_KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')
DEST_KAFKA_TOPIC = os.getenv('DEST_KAFKA_TOPIC', 'raw-events')
DEST_KAFKA_USERNAME = os.getenv('DEST_KAFKA_USERNAME', 'kafka')
DEST_KAFKA_PASSWORD = os.getenv('DEST_KAFKA_PASSWORD')

# MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'ecommerce')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'product_views')

# Consumer
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'mongo-consumer-group')