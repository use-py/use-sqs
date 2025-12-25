import logging
import json
import os
import threading
import time
from typing import Callable, Optional, Union, Any, Dict, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class SQSStore:
    """
    AWS SQS消息队列存储和消费类

    提供了与AWS SQS交互的各种方法,包括连接、声明队列、发送消息、获取消息数量和消费消息等。
    包含了重试机制和异常处理,以确保连接的可靠性和消息的正确传递。

    队列类型说明：
    - 标准队列：队列名不以.fifo结尾（大小写不敏感）
    - FIFO队列：队列名以.fifo结尾（大小写不敏感）
    """

    MAX_RETRIES = 5
    MAX_RECONNECTION_DELAY = 32
    INITIAL_RECONNECTION_DELAY = 1

    def __init__(
        self,
        *,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        **kwargs,
    ):
        """
        初始化SQS客户端

        :param region_name: AWS region
        :param aws_access_key_id: AWS access key
        :param aws_secret_access_key: AWS secret key
        :param endpoint_url: SQS endpoint URL (用于本地测试)
        :param kwargs: 其他boto3参数
        """
        self._shutdown = False
        self._reconnection_delay = self.INITIAL_RECONNECTION_DELAY
        self._client = None
        self._queues = {}

        # 构建boto3参数
        self.parameters = {
            "region_name": region_name or os.environ.get("AWS_REGION", "us-east-1"),
            "aws_access_key_id": aws_access_key_id
            or os.environ.get("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": aws_secret_access_key
            or os.environ.get("AWS_SECRET_ACCESS_KEY"),
        }
        if endpoint_url:
            self.parameters["endpoint_url"] = endpoint_url
        if kwargs:
            self.parameters.update(kwargs)

    @property
    def client(self):
        """获取或创建SQS客户端"""
        if self._client is None:
            self._client = boto3.resource("sqs", **self.parameters)
        return self._client

    def declare_queue(
        self,
        queue_name: str,
        attributes: Optional[Dict[str, str]] = None,
        auto_create: bool = True,
    ) -> Any:
        """
        声明或获取队列

        :param queue_name: 队列名称
                        以.fifo结尾表示FIFO队列，否则为标准队列
        :param attributes: 队列属性
        :param auto_create: 是否自动创建
        :return: SQS队列对象
        """
        if queue_name in self._queues:
            return self._queues[queue_name]

        for attempt in range(self.MAX_RETRIES):
            try:
                queue = self.client.get_queue_by_name(QueueName=queue_name)  # pyright: ignore[reportAttributeAccessIssue]
                self._queues[queue_name] = queue
                return queue
            except ClientError as e:
                if (
                    e.response["Error"]["Code"]
                    == "AWS.SimpleQueueService.NonExistentQueue"
                ):
                    if auto_create:
                        attributes = attributes or {}
                        if queue_name.lower().endswith(".fifo"):
                            attributes.setdefault("FifoQueue", "true")
                            attributes.setdefault("ContentBasedDeduplication", "true")
                        queue = self.client.create_queue(  # pyright: ignore[reportAttributeAccessIssue]
                            QueueName=queue_name, Attributes=attributes
                        )
                        self._queues[queue_name] = queue
                        return queue
                    raise
                elif attempt < self.MAX_RETRIES - 1:
                    delay = min(
                        self._reconnection_delay * (2**attempt),
                        self.MAX_RECONNECTION_DELAY,
                    )
                    logger.warning(f"Failed to get queue, retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                raise

    def _convert_message_params(self, params: dict, queue_name: str) -> dict:
        """
        转换消息参数为AWS SQS格式

        支持的参数映射关系:
        标准队列和FIFO队列通用参数:
        - message_attributes -> MessageAttributes
        - message_system_attributes -> MessageSystemAttributes

        FIFO队列专用参数:
        - message_group_id -> MessageGroupId (必需)
        - message_deduplication_id -> MessageDeduplicationId (可选)

        注意：
        1. 标准队列不支持的FIFO队列参数(MessageGroupId, MessageDeduplicationId)会被忽略并打印警告
        2. 消息延迟(DelaySeconds)只能在SQS队列管理页面设置，此参数会被忽略并打印警告
        """
        result = {}
        param_mapping = {
            "message_attributes": "MessageAttributes",
            "message_system_attributes": "MessageSystemAttributes",
            "message_group_id": "MessageGroupId",
            "message_deduplication_id": "MessageDeduplicationId",
        }

        # 转换参数名称
        for key, value in params.items():
            if key in param_mapping:
                result[param_mapping[key]] = value
            elif key in ["delay_seconds", "DelaySeconds"]:
                logger.warning(
                    "DelaySeconds is not supported for individual messages, it can only be set at queue level"
                )
                continue
            else:
                result[key] = value

        is_fifo = queue_name.lower().endswith(".fifo")

        # FIFO队列参数验证
        if is_fifo:
            # MessageGroupId是必需的
            group_id = result.get("MessageGroupId")
            if not group_id:
                group_id = queue_name.rsplit(".", 1)[0]
            if not isinstance(group_id, str) or len(group_id) > 128:
                raise ValueError(
                    "MessageGroupId must be a string of at most 128 characters"
                )
            result["MessageGroupId"] = group_id

            # MessageDeduplicationId是可选的
            dedup_id = result.get("MessageDeduplicationId")
            if dedup_id:
                if not isinstance(dedup_id, str) or len(dedup_id) > 128:
                    raise ValueError(
                        "MessageDeduplicationId must be a string of at most 128 characters"
                    )

        # 标准队列参数验证
        else:
            # 移除FIFO队列专用参数
            if "MessageGroupId" in result:
                logger.warning(
                    "MessageGroupId is only supported for FIFO queues, the parameter will be ignored"
                )
                result.pop("MessageGroupId")

            if "MessageDeduplicationId" in result:
                logger.warning(
                    "MessageDeduplicationId is only supported for FIFO queues, the parameter will be ignored"
                )
                result.pop("MessageDeduplicationId")

        # 处理消息属性 - 只校验格式
        if "MessageAttributes" in result:
            attrs = result["MessageAttributes"]
            if not isinstance(attrs, dict):
                raise ValueError("MessageAttributes must be a dictionary")
            for name, attr in attrs.items():
                if not isinstance(attr, dict):
                    raise ValueError(f"MessageAttribute {name} must be a dictionary")
                if "DataType" not in attr:
                    raise ValueError(f"MessageAttribute {name} must have DataType")

        # 处理系统属性 - 只校验格式
        if "MessageSystemAttributes" in result:
            sys_attrs = result["MessageSystemAttributes"]
            if not isinstance(sys_attrs, dict):
                raise ValueError("MessageSystemAttributes must be a dictionary")
            for name, attr in sys_attrs.items():
                if not isinstance(attr, dict):
                    raise ValueError(
                        f"MessageSystemAttribute {name} must be a dictionary"
                    )
                if "DataType" not in attr:
                    raise ValueError(
                        f"MessageSystemAttribute {name} must have DataType"
                    )

        return result

    def send(
        self,
        queue_name: str,
        message: Union[str, bytes, dict],
        **kwargs,
    ) -> Any:
        """
        发送消息到指定队列

        :param queue_name: 队列名称
        :param message: 消息内容，支持字符串、字节或字典类型
        :param kwargs: 其他发送参数，支持驼峰和蛇形命名，例如:
                    - message_attributes 或 MessageAttributes: 消息属性，格式如下:
                        {
                            "my_attribute": {
                                "DataType": "String",
                                "StringValue": "my_value"
                            }
                        }
                    - message_system_attributes 或 MessageSystemAttributes: 系统属性，格式如下:
                        {
                            "AWSTraceHeader": {
                                "DataType": "String",
                                "StringValue": "trace_id"
                            }
                        }
                    - message_group_id 或 MessageGroupId: 消息组ID(FIFO队列必需)
                    - message_deduplication_id 或 MessageDeduplicationId: 消息去重ID(FIFO队列可选)

                    注意：不支持的参数会被自动忽略并打印警告信息
        :return: 发送的消息

        :raises:
            ValueError: 参数验证失败
            ClientError: AWS SQS API调用失败

        :example:
            # 标准队列示例
            sqs.send("my_queue", "Hello World")

            # FIFO队列示例
            sqs.send(
                "my_queue.fifo",
                {"data": "test"},
                message_group_id="group1",
                message_attributes={
                    "my_attribute": {
                        "DataType": "String",
                        "StringValue": "my_value"
                    }
                }
            )

            # 使用不支持的参数（会被忽略）
            sqs.send(
                "my_queue",
                "Hello World",
                message_group_id="group1"  # 不支持的参数会被忽略并打印警告
            )
        """
        # 验证消息内容
        if isinstance(message, dict):
            message_body = json.dumps(message)
        elif isinstance(message, bytes):
            message_body = message.decode("utf-8")
        else:
            message_body = str(message)

        # 验证Unicode字符
        try:
            message_body.encode("utf-8").decode("utf-8")
        except UnicodeError:
            raise ValueError("Message contains invalid Unicode characters")

        send_params = self._convert_message_params(kwargs, queue_name)

        for attempt in range(self.MAX_RETRIES):
            try:
                self.declare_queue(queue_name).send_message(
                    MessageBody=message_body, **send_params
                )
                return message

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "InvalidMessageContents":
                    raise ValueError("Message contains invalid characters")
                elif error_code == "MessageNotFound":
                    raise ValueError("Message not found")
                elif error_code == "QueueDoesNotExist":
                    raise ValueError(f"Queue {queue_name} does not exist")
                elif error_code == "RequestThrottled":
                    if attempt < self.MAX_RETRIES - 1:
                        delay = min(
                            self._reconnection_delay * (2**attempt),
                            self.MAX_RECONNECTION_DELAY,
                        )
                        logger.warning(f"Request throttled, retrying in {delay}s...")
                        time.sleep(delay)
                        continue
                raise
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    delay = min(
                        self._reconnection_delay * (2**attempt),
                        self.MAX_RECONNECTION_DELAY,
                    )
                    logger.warning(
                        f"Failed to send message, retrying in {delay}s...({e=})"
                    )
                    time.sleep(delay)
                    continue
                raise

    def get_message_counts(self, queue_name: str) -> int:
        """
        获取队列中的消息数量

        :param queue_name: 队列名称
        :return: 消息数量
        """
        try:
            queue = self.declare_queue(queue_name)
            attrs = queue.attributes
            return int(attrs.get("ApproximateNumberOfMessages", 0))
        except Exception as e:
            logger.error(f"Failed to get message count: {e}")
            return 0

    def flush_queue(self, queue_name: str):
        """
        清空队列中的所有消息

        :param queue_name: 队列名称
        """
        queue = self.declare_queue(queue_name)
        while True:
            messages = queue.receive_messages(MaxNumberOfMessages=10)
            if not messages:
                break
            for msg in messages:
                msg.delete()

    def receive_messages(
        self,
        queue_name: str,
        *,
        max_number_of_messages: int = 1,
        wait_time_seconds: int = 0,
        no_ack: bool = False,
        **kwargs,
    ) -> List[Any]:
        """
        从队列中接收消息
        :param queue_name: 队列名称
        :param max_number_of_messages: 一次最多接收的消息数量，默认为1
        :param wait_time_seconds: 长轮询等待时间（秒），默认为0（不使用长轮询）, 取值范围为0-20
        :param kwargs: 其他AWS SQS支持的参数
        :return: 接收到的消息列表
        """
        queue = self.declare_queue(queue_name)
        messages = queue.receive_messages(
            MaxNumberOfMessages=max_number_of_messages,
            WaitTimeSeconds=wait_time_seconds,
            **kwargs,
        )
        return messages

    def start_consuming(
        self,
        queue_name: str,
        callback: Callable,
        prefetch: int = 1,
        wait_time: int = 20,
        no_ack: bool = False,
        **kwargs,
    ):
        """
        开始消费队列消息

        :param queue_name: 队列名称
        :param callback: 消息处理回调函数，参数为SQS Message对象
        :param prefetch: 预取消息数量，默认为1
                    - FIFO队列：同一MessageGroupId的消息会顺序处理
                    - 标准队列：可以设置更大的值来实现批量处理
        :param wait_time: 长轮询等待时间（秒），默认20
        :param no_ack: 是否自动确认
        :param kwargs: 其他AWS SQS支持的参数
        """
        self._shutdown = False
        reconnection_delay = self.INITIAL_RECONNECTION_DELAY
        queue = self.declare_queue(queue_name)

        while not self._shutdown:
            try:
                messages = queue.receive_messages(
                    MaxNumberOfMessages=prefetch,
                    AttributeNames=["All"],
                    WaitTimeSeconds=wait_time,
                    **kwargs,
                )

                for message in messages:
                    try:
                        # 测试是否能解析JSON
                        body = message.body
                        try:
                            body = json.loads(body)
                        except json.JSONDecodeError:
                            pass

                        callback(message)

                        # 根据no_ack参数决定是否自动删除消息
                        if no_ack:
                            message.delete()
                    except Exception as e:
                        logger.exception(f"Error processing message: {e}")

                if not messages:
                    time.sleep(1)  # 避免空队列时频繁请求

            except Exception as e:
                if self._shutdown:
                    break
                logger.exception(f"SQSStore consume error: {e}, reconnecting...")
                self._client = None
                time.sleep(reconnection_delay)
                reconnection_delay = min(
                    reconnection_delay * 2, self.MAX_RECONNECTION_DELAY
                )
                queue = self.declare_queue(queue_name)

    def listener(self, queue_name: str, no_ack: bool = False, **kwargs):
        """
        消息监听器装饰器

        :param queue_name: 队列名称
        :param no_ack: 是否自动确认
        :param kwargs: 其他参数
        """

        def wrapper(callback: Callable[[Any], Any]):
            logger.info(f"Starting SQS consumer for queue: {queue_name}")

            def target():
                self.start_consuming(queue_name, callback, no_ack=no_ack, **kwargs)

            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            return thread

        return wrapper

    def shutdown(self):
        """关闭连接"""
        self._shutdown = True
        self._client = None
        self._queues.clear()

    def delete_queue(self, queue_name: str):
        """
        删除队列

        :param queue_name: 队列名称
        """
        try:
            queue = self.declare_queue(queue_name, auto_create=False)
            queue.delete()
            self._queues.pop(queue_name, None)
        except Exception as e:
            logger.error(f"Failed to delete queue {queue_name}: {e}")
            raise


class SQSListener:
    """SQS监听器类"""

    def __init__(
        self, instance: SQSStore, *, queue_name: str, no_ack: bool = False, **kwargs
    ):
        """
        初始化监听器

        :param instance: SQSStore实例
        :param queue_name: 队列名称
        :param no_ack: 是否自动确认
        :param kwargs: 其他参数
        """
        self.instance = instance
        self.queue_name = queue_name
        self.no_ack = no_ack
        self.kwargs = kwargs

    def __call__(self, callback: Callable[[Any], None]):
        """
        调用监听器

        :param callback: 回调函数
        :return: 监听线程
        """
        listener = self.instance.listener(self.queue_name, self.no_ack, **self.kwargs)
        return listener(callback)


# 别名
useSQS = SQSStore
useSQSListener = SQSListener
