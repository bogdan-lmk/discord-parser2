import requests
import json
import os
from datetime import datetime
from time import sleep
from loguru import logger
from sys import stderr
from translatepy.translators.google import GoogleTranslate
from dotenv import load_dotenv
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white> | <level>{level: <8}</level> | <cyan>{line}</cyan> - <white>{message}</white>")

class DiscordParser:
    def __init__(self):
        self.sessions = []
        self.gtranslate = GoogleTranslate()
        
        # Initialize sessions for each token
        for token in config.DISCORD_TOKENS:
            session = requests.Session()
            session.headers['authorization'] = token
            self.sessions.append(session)
    
    def parse_announcement_channel(self, channel_id, server_name=None, channel_name=None):
        """Parse messages from an announcement channel"""
        messages = []
        for session in self.sessions:
            try:
                last_id = None
                while True:
                    url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
                    if last_id:
                        url += f'?before={last_id}'
                    
                    r = session.get(url)
                    if r.status_code != 200:
                        logger.error(f"Failed to fetch messages: {r.text}")
                        break
                        
                    batch = json.loads(r.text)
                    if not batch:
                        break
                        
                    for msg in batch:
                        message = Message(
                            content=msg['content'],
                            timestamp=datetime.fromisoformat(msg['timestamp']),
                            server_name=server_name or msg.get('guild_id'),
                            channel_name=channel_name or msg['channel_id'],
                            author=msg['author']['username']
                        )
                        messages.append(message)
                        last_id = msg['id']
                        
                    sleep(0.5)  # Rate limiting
                    
            except Exception as e:
                logger.error(f"Error parsing channel {channel_id}: {e}")
                continue
                
        return messages
    
    def save_messages(self, messages, filename='messages.json'):
        """Save messages to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump([msg.__dict__ for msg in messages], f, indent=2, default=str)
        logger.success(f"Saved {len(messages)} messages to {filename}")

if __name__ == '__main__':
    parser = DiscordParser()
    
    # Example usage - would be replaced with proper channel discovery
    channel_id = input("Enter announcement channel ID: ")
    server_name = input("Enter server name (optional): ")
    channel_name = input("Enter channel name (optional): ")
    
    messages = parser.parse_announcement_channel(channel_id, server_name, channel_name)
    parser.save_messages(messages)
