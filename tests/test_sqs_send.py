import unittest
import json
from unittest.mock import MagicMock, patch
from use_sqs import SQSStore
from botocore.exceptions import ClientError

class TestSQSStoreSend(unittest.TestCase):
    def setUp(self):
        # 初始化 SQSStore，使用虚假的凭证
        self.sqs = SQSStore(
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test"
        )
        # Mock boto3 resource client
        self.sqs._client = MagicMock()
        
        # 创建一个 Mock 的 Queue 对象
        self.mock_queue = MagicMock()
        # 让 get_queue_by_name 返回这个 Mock Queue
        self.sqs._client.get_queue_by_name.return_value = self.mock_queue
        # 也可以直接预填充 _queues 缓存，绕过 declare_queue 的查找过程
        self.sqs._queues["test-queue"] = self.mock_queue

    def test_send_string_message(self):
        """测试发送普通字符串消息"""
        message = "hello world"
        result = self.sqs.send("test-queue", message)
        
        # 验证返回值是否为原消息
        self.assertEqual(result, message)
        # 验证底层 send_message 调用参数
        self.mock_queue.send_message.assert_called_with(MessageBody=message)

    def test_send_dict_message(self):
        """测试发送字典消息（自动转换为JSON）"""
        message = {"key": "value", "num": 123}
        self.sqs.send("test-queue", message)
        
        # 验证字典被序列化为 JSON 字符串
        self.mock_queue.send_message.assert_called_with(MessageBody=json.dumps(message))

    def test_send_bytes_message(self):
        """测试发送字节消息"""
        message = b"bytes data"
        self.sqs.send("test-queue", message)
        
        # 验证字节被解码为字符串
        self.mock_queue.send_message.assert_called_with(MessageBody="bytes data")

    def test_send_with_message_attributes(self):
        """测试带属性的消息发送"""
        attrs = {
            "MyAttr": {
                "DataType": "String",
                "StringValue": "MyValue"
            }
        }
        self.sqs.send("test-queue", "msg", message_attributes=attrs)
        
        self.mock_queue.send_message.assert_called_with(
            MessageBody="msg",
            MessageAttributes=attrs
        )

    def test_send_fifo_queue(self):
        """测试发送到 FIFO 队列"""
        queue_name = "my-queue.fifo"
        # 模拟 FIFO 队列对象
        mock_fifo_queue = MagicMock()
        self.sqs._queues[queue_name] = mock_fifo_queue
        
        # 指定 MessageGroupId
        self.sqs.send(queue_name, "msg", message_group_id="group1")
        
        mock_fifo_queue.send_message.assert_called_with(
            MessageBody="msg",
            MessageGroupId="group1"
        )

    def test_send_fifo_auto_group_id(self):
        """测试 FIFO 队列自动生成 MessageGroupId"""
        queue_name = "auto-group.fifo"
        mock_fifo_queue = MagicMock()
        self.sqs._queues[queue_name] = mock_fifo_queue
        
        # 未指定 MessageGroupId，应自动使用队列名（去掉 .fifo 后缀）
        self.sqs.send(queue_name, "msg")
        
        mock_fifo_queue.send_message.assert_called_with(
            MessageBody="msg",
            MessageGroupId="auto-group"
        )

    def test_send_standard_queue_ignores_fifo_params(self):
        """测试标准队列自动忽略 FIFO 参数"""
        # 向标准队列发送带 group_id 的消息
        self.sqs.send("test-queue", "msg", message_group_id="group1")
        
        # 验证 MessageGroupId 被移除
        call_kwargs = self.mock_queue.send_message.call_args[1]
        self.assertNotIn("MessageGroupId", call_kwargs)
        self.assertEqual(call_kwargs["MessageBody"], "msg")

    def test_send_invalid_unicode(self):
        """测试无效 Unicode 字符抛出异常"""
        # 构造一个包含无效 utf-8 序列的字节串
        invalid_bytes = b'\x80' 
        with self.assertRaises(ValueError):
            self.sqs.send("test-queue", invalid_bytes)

    def test_client_error_handling(self):
        """测试 AWS ClientError 处理"""
        # 模拟 AWS 抛出异常
        error_response = {'Error': {'Code': 'InvalidMessageContents', 'Message': 'Invalid chars'}}
        self.mock_queue.send_message.side_effect = ClientError(error_response, 'SendMessage')
        
        with self.assertRaisesRegex(ValueError, "Message contains invalid characters"):
            self.sqs.send("test-queue", "bad msg")

    def test_send_message_too_large(self):
        """测试消息超过 256KB 抛出异常"""
        # 构造一个刚刚超过 256KB 的消息
        # 262144 bytes is exactly 256KB
        large_msg = "a" * (262144 + 1)
        
        with self.assertRaisesRegex(ValueError, "Message size \d+ exceeds the maximum allowed size of 262144 bytes"):
            self.sqs.send("test-queue", large_msg)

if __name__ == '__main__':
    unittest.main()
