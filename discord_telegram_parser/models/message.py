from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Message:
    content: str
    timestamp: Optional[datetime] = None
    server_name: Optional[str] = None
    channel_name: Optional[str] = None
    author: Optional[str] = None
    translated_content: Optional[str] = None
