from core.use_sns import useSNS
from config import settings

# 初始化 SNS 客户端
sns = useSNS(**settings.sns.to_dict())

# 你的 SNS Topic ARN
TOPIC_ARN = "arn:aws-cn:sns:cn-northwest-1:928507961548:ceegic-event-bus.fifo"


def send_filtered_event():
    """
    发送一条符合订阅者过滤策略的消息
    """
    message = {
        "event": "project_created",
        "project_id": 1001,
        "name": "New Project",
        "timestamp": "2025-04-05T12:34:56Z",
    }

    # 添加 MessageAttributes，用于匹配订阅者的 FilterPolicy
    message_attributes = {
        "event-type": {
            "DataType": "String",
            "StringValue": "project",  # 必须匹配订阅的 event-type
        }
    }

    try:
        response = sns.publish(
            topic_arn=TOPIC_ARN,
            message=message,
            message_group_id="project_events",  # FIFO 必需
            message_attributes=message_attributes,
        )
        print("✅ Message published successfully.")
        print("MessageId:", response["MessageId"])
    except Exception as e:
        print("❌ Failed to publish message:", str(e))


if __name__ == "__main__":
    send_filtered_event()
