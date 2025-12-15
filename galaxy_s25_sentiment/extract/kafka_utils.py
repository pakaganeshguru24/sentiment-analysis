"""Kafka utilities for real-time sentiment analysis."""

import json
import os
from typing import Dict, List

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:
    KafkaConsumer = None
    KafkaProducer = None


class KafkaManager:
    """Manages Kafka producer and consumer operations."""
    
    def __init__(self, bootstrap_servers: str = None, topic: str = None):
        self.bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        self.topic = topic or os.environ.get("KAFKA_TOPIC", "sentiment-stream")
        self.producer = None
        self.consumer = None
    
    def get_producer(self) -> 'KafkaProducer':
        """Get or create Kafka producer."""
        if self.producer is None:
            if KafkaProducer is None:
                raise RuntimeError("kafka-python not installed")
            
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    retries=3,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to create Kafka producer: {e}")
        
        return self.producer
    
    def get_consumer(self, group_id: str = "sentiment-dashboard") -> 'KafkaConsumer':
        """Get or create Kafka consumer."""
        if self.consumer is None:
            if KafkaConsumer is None:
                raise RuntimeError("kafka-python not installed")
            
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to create Kafka consumer: {e}")
        
        return self.consumer
    
    def send_message(self, data: Dict) -> bool:
        """Send a single message to Kafka."""
        try:
            producer = self.get_producer()
            producer.send(self.topic, value=data)
            producer.flush()
            return True
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
            return False
    
    def consume_messages(self, max_records: int = 100) -> List[Dict]:
        """Consume messages from Kafka."""
        try:
            consumer = self.get_consumer()
            messages = []
            polled_data = consumer.poll(timeout_ms=500, max_records=max_records)
            
            for partition_records in polled_data.values():
                for message in partition_records:
                    messages.append(message.value)
            
            return messages
        except Exception as e:
            print(f"Error consuming from Kafka: {e}")
            return []
    
    def close(self):
        """Close producer and consumer."""
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()