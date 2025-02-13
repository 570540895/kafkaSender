from kafka import KafkaProducer
import json

# Kafka集群的地址
kafka_servers = ['10.200.88.46:9092', '10.200.88.47:9092', '10.200.88.50:9092']

# 目标Topic
topic_name = 'dros_node_fault_detect_simulation'

# 创建Kafka生产者
producer = KafkaProducer(
    bootstrap_servers=kafka_servers,  # Kafka集群地址
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 序列化消息为JSON
)

# 要发送的消息
message = {
    "id": 33,
    "cluster": "nanhu_gpu3_test",
    "nodeName": "sm228-gpu-40",
    "ip": "101.106.82.160",
    "sn": "PR4785TP201208SZ00038",
    "idc": "N-M130",
    "cabinet": "N-M130|H06",
    "uPosition": "N-M130|H06|20",
    "startTime": "2025-02-13 00:00:00",
    "endTime": "2024-02-13 15:54:38",
    "status": "0",
    "criticalMessages": [{"reason":"测试","message":"故障描述","status":"3","startTime":"2024-12-04 00:00:00","endTime":"2024-12-05 15:54:38"}]
}

# 发送消息到Kafka
producer.send(topic_name, value=message)

# 刷新生产者，确保消息发送完成
producer.flush()

print(f"Message sent to topic '{topic_name}' successfully!")

# 关闭生产者连接
producer.close()
