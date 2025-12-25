import logging
import json
import os
import time
import uuid
from typing import Optional, Union, Dict
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class SNSPublisher:
    """
    AWS SNS消息发布类
    提供了与AWS SNS交互的方法，包括初始化客户端、发布消息等。
    支持FIFO主题（.fifo结尾）的消息组ID和去重ID设置。
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
        初始化SNS客户端
        :param region_name: AWS region
        :param aws_access_key_id: AWS access key
        :param aws_secret_access_key: AWS secret key
        :param endpoint_url: SNS endpoint URL (用于本地测试)
        :param kwargs: 其他boto3参数
        """
        self._client = None
        self._reconnection_delay = self.INITIAL_RECONNECTION_DELAY

        # 构建 boto3 参数
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
        """获取或创建SNS客户端"""
        if self._client is None:
            self._client = boto3.client("sns", **self.parameters)
        return self._client

    def _convert_message_params(self, params: dict, topic_arn: str) -> dict:
        """
        转换消息参数为AWS SNS格式
        支持的参数映射关系:
        - message_attributes -> MessageAttributes
        - message_group_id -> MessageGroupId (仅FIFO主题支持)
        - message_deduplication_id -> MessageDeduplicationId (可选)
        注意：
        - 标准主题忽略MessageGroupId并打印警告
        - 如果未指定MessageDeduplicationId且未启用ContentBasedDeduplication，则自动生成UUID
        """
        result = {}
        param_mapping = {
            "message_attributes": "MessageAttributes",
            "message_group_id": "MessageGroupId",
            "message_deduplication_id": "MessageDeduplicationId",
        }

        for key, value in params.items():
            if key in param_mapping:
                result[param_mapping[key]] = value
            else:
                result[key] = value

        is_fifo = ".fifo" in topic_arn.lower()

        # FIFO主题参数验证
        if is_fifo:
            # MessageGroupId 是必需的
            group_id = result.get("MessageGroupId")
            if not group_id:
                group_id = str(uuid.uuid4())[:128]
            if not isinstance(group_id, str) or len(group_id) > 128:
                raise ValueError(
                    "MessageGroupId must be a string of at most 128 characters"
                )
            result["MessageGroupId"] = group_id

            # MessageDeduplicationId 可选，若未提供则生成
            dedup_id = result.get("MessageDeduplicationId")
            if not dedup_id:
                result["MessageDeduplicationId"] = str(uuid.uuid4())
            elif not isinstance(dedup_id, str) or len(dedup_id) > 128:
                raise ValueError(
                    "MessageDeduplicationId must be a string of at most 128 characters"
                )
        else:
            # 标准主题不支持 MessageGroupId 和 MessageDeduplicationId
            if "MessageGroupId" in result:
                logger.warning(
                    "MessageGroupId is only supported for FIFO topics, the parameter will be ignored"
                )
                result.pop("MessageGroupId")
            if "MessageDeduplicationId" in result:
                logger.warning(
                    "MessageDeduplicationId is only supported for FIFO topics, the parameter will be ignored"
                )
                result.pop("MessageDeduplicationId")

        # 校验 MessageAttributes 格式
        if "MessageAttributes" in result:
            attrs = result["MessageAttributes"]
            if not isinstance(attrs, dict):
                raise ValueError("MessageAttributes must be a dictionary")
            for name, attr in attrs.items():
                if not isinstance(attr, dict):
                    raise ValueError(f"MessageAttribute {name} must be a dictionary")
                if "DataType" not in attr:
                    raise ValueError(f"MessageAttribute {name} must have DataType")
        return result

    def publish(
        self,
        topic_arn: str,
        message: Union[str, bytes, dict],
        **kwargs,
    ) -> Dict:
        """
        发布消息到指定的SNS主题
        :param topic_arn: SNS主题ARN
        :param message: 消息内容，支持字符串、字节或字典类型
        :param kwargs: 其他发送参数，例如:
                    - message_attributes 或 MessageAttributes
                    - message_group_id 或 MessageGroupId (FIFO 必需)
                    - message_deduplication_id 或 MessageDeduplicationId (FIFO 可选)
        :return: AWS响应结果
        :raises:
            ValueError: 参数验证失败
            ClientError: AWS API调用失败
        :example:
            sns.publish(
                "arn:aws:sns:region:account:my-topic.fifo",
                {"event": "user_created"},
                message_group_id="group1"
            )
        """
        # 处理消息体
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

        send_params = self._convert_message_params(kwargs, topic_arn)

        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.client.publish(
                    TopicArn=topic_arn,
                    Message=message_body,
                    **send_params,
                )
                return response
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "InvalidMessageContents":
                    raise ValueError("Message contains invalid characters")
                elif error_code == "TopicNotFound":
                    raise ValueError(f"Topic {topic_arn} does not exist")
                elif error_code == "RequestThrottled":
                    if attempt < self.MAX_RETRIES - 1:
                        delay = min(
                            self._reconnection_delay * (2**attempt),
                            self.MAX_RECONNECTION_DELAY,
                        )
                        logger.warning(f"Request throttled, retrying in {delay}s...")
                        time.sleep(delay)
                        continue
                else:
                    raise
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    delay = min(
                        self._reconnection_delay * (2**attempt),
                        self.MAX_RECONNECTION_DELAY,
                    )
                    logger.warning(
                        f"Failed to publish message, retrying in {delay}s... ({e})"
                    )
                    time.sleep(delay)
                    continue
                raise
        return {}


# 别名
useSNS = SNSPublisher
