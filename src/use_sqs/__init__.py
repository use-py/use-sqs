"""
use-sqs - A framework-agnostic template library.
"""

__version__ = "0.1.0"

from .use_sqs import SQSStore, SQSListener, useSQS, useSQSListener
from .use_sns import SNSPublisher, useSNS

__all__ = (
    "SQSStore",
    "SQSListener",
    "SNSPublisher",
    "useSQS",
    "useSQSListener",
    "useSNS",
    "__version__",
)
