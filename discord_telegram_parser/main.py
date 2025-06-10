import requests
import json
import os
from datetime import datetime
from time import sleep
from loguru import logger
from translatepy.translators.google import GoogleTranslate
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config

class DiscordParser:
    def __init__(self):
        self.sessions = []
        self.gtranslate = GoogleTranslate()
        
        # Initialize sessions for each token
        for token in config.DISCORD_TOKENS:
            session = requests.Session()
            session.headers['authorization'] = token
            
            # Verify token permissions
            try:
                r = session.get('https://discord.com/api/v9/users/@me')
                if r.status_code != 200:
                    raise Exception(f"Invalid token (HTTP {r.status_code})")
                    
                user_info = json.loads(r.text)
                print(f"Using token for: {user_info.get('username')}")
                
                # Check guild permissions
                r = session.get('https://discord.com/api/v9/users/@me/guilds')
                if r.status_code != 200:
                    raise Exception(f"Can't access guilds (HTTP {r.status_code})")
                    
            except Exception as e:
                print(f"Error validating token: {e}")
                continue
                
            self.sessions.append(session)
    
    def parse_announcement_channel(self, channel_id, server_name=None, channel_name=None, limit=10):
        """Parse the last N messages from an announcement channel with token rotation"""
        messages = []
        token_index = 0
        has_more = True
        last_id = None
        count = 0
        
        while has_more:
            session = self.sessions[token_index]
            try:
                # Fetch messages in batches of 10
                params = {'limit': min(10, limit - count) if limit else 10}
                if last_id:
                    params['before'] = last_id
                    
                # Stop if we've reached the limit
                if limit and count >= limit:
                    break
                    
                r = session.get(
                    f'https://discord.com/api/v9/channels/{channel_id}/messages',
                    params=params
                )
                
                if r.status_code == 200:
                    batch = r.json()
                    if not batch:
                        has_more = False
                        break
                        
                    # Process messages in reverse chronological order (newest first)
                    for msg in batch:
                        msg_time = datetime.fromisoformat(msg['timestamp'])
                        
                        # Sanitize message content
                        content = msg['content'].encode('utf-8', 'replace').decode('utf-8')
                        author = msg['author']['username'].encode('utf-8', 'replace').decode('utf-8')
                        server = server_name.encode('utf-8', 'replace').decode('utf-8') if server_name else None
                        channel = channel_name.encode('utf-8', 'replace').decode('utf-8') if channel_name else None
                        
                        message = Message(
                            content=content,
                            timestamp=msg_time,
                            server_name=server,
                            channel_name=channel,
                            author=author
                        )
                        messages.append(message)
                        last_id = msg['id']
                        count += 1
                        
                        # Stop if we've reached the limit
                        if limit and count >= limit:
                            break
                    
                    # If we broke early due to timestamp, exit
                    if not has_more:
                        break
                        
                    sleep(0.2)  # Avoid rate limits
                    
                elif r.status_code == 429:  # Rate limited
                    retry_after = r.headers.get('Retry-After', 5)
                    logger.warning(f"Rate limited - retrying in {retry_after}s")
                    sleep(int(retry_after))
                    continue
                    
                elif r.status_code == 403:
                    logger.error(f"Access denied to channel {channel_id}")
                    # Rotate token
                    token_index = (token_index + 1) % len(self.sessions)
                    logger.info(f"Rotating to token {token_index}")
                    continue
                    
                else:
                    error_details = r.json().get('message', 'Unknown error')
                    logger.error(f"Failed to fetch messages (HTTP {r.status_code}): {error_details}")
                    # Rotate token
                    token_index = (token_index + 1) % len(self.sessions)
                    logger.info(f"Rotating to token {token_index}")
                    
            except Exception as e:
                logger.error(f"Error parsing channel {channel_id}: {e}")
                # Rotate token on any exception
                token_index = (token_index + 1) % len(self.sessions)
                logger.info(f"Rotating to token {token_index} after exception")
                
        # Return messages in chronological order (oldest first)
        return sorted(messages, key=lambda x: x.timestamp)
        
    def sanitize_string(self, s):
        """Helper to fix encoding issues"""
        return s.encode('utf-8', 'replace').decode('utf-8')
    
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
