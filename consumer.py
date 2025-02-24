from kafka import KafkaConsumer
import json

kafka_cfg_file = 'kafka-config.json'

with open(kafka_cfg_file, 'r') as fp:
    kafka_cfg_dict = json.load(fp)
    kafka_servers = kafka_cfg_dict['servers']
    topic_name = kafka_cfg_dict['topic']

# 创建一个 Kafka 消费者
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=kafka_servers,  # Kafka集群地址
    auto_offset_reset='earliest',  # 从最早的消息开始消费
    group_id='my_consumer_group',  # 消费者组ID
    value_deserializer=lambda m: m.decode('utf-8')  # 消息的反序列化方式，这里假设消息是字符串
)

# 消费消息
try:
    print(f"Consuming messages from topic: {topic_name}")
    for message in consumer:
        print(f"Received message: {message.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    # 关闭消费者连接
    consumer.close()
