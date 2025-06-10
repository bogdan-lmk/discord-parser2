import requests
import json
import os
import time
from dotenv import load_dotenv

class DiscordIDCollector:
    def __init__(self, token):
        self.token = token
        self.session = requests.Session()
        self.session.headers = {'Authorization': self.token}
        self.servers_data = {}

    def get_guilds(self):
        """Get list of guilds/servers the user is in with pagination"""
        guilds = []
        url = 'https://discord.com/api/v9/users/@me/guilds'
        
        while url:
            try:
                r = self.session.get(url)
                if r.status_code == 200:
                    guilds.extend(json.loads(r.text))
                    # Check for pagination
                    if 'Link' in r.headers:
                        links = r.headers['Link'].split(',')
                        for link in links:
                            if 'rel="next"' in link:
                                url = link[link.find('<')+1:link.find('>')]
                                break
                            else:
                                url = None
                    else:
                        url = None
                elif r.status_code == 429:  # Rate limited
                    retry_after = float(r.json().get('retry_after', 1))
                    print(f"Rate limited, waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    print(f"Warning: Failed to get guilds (status {r.status_code})")
                    url = None
            except Exception as e:
                print(f"Error getting guilds: {str(e)}")
                url = None
                
        return guilds

    def get_guild_channels(self, guild_id):
        """Get channels for a specific guild with pagination and rate limit handling"""
        channels = []
        url = f'https://discord.com/api/v9/guilds/{guild_id}/channels'
        
        while url:
            try:
                r = self.session.get(url)
                if r.status_code == 200:
                    channels.extend(json.loads(r.text))
                    # Check for pagination
                    if 'Link' in r.headers:
                        links = r.headers['Link'].split(',')
                        for link in links:
                            if 'rel="next"' in link:
                                url = link[link.find('<')+1:link.find('>')]
                                break
                            else:
                                url = None
                    else:
                        url = None
                elif r.status_code == 429:  # Rate limited
                    retry_after = float(r.json().get('retry_after', 1))
                    print(f"Rate limited, waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    print(f"Warning: Failed to get channels for guild {guild_id} (status {r.status_code})")
                    url = None
            except Exception as e:
                print(f"Error getting channels for guild {guild_id}: {str(e)}")
                url = None
                
        return channels

    def collect_ids(self, server_id=None):
        """Main collection method using HTTP API
        Args:
            server_id: Optional specific server ID to fetch
        Returns: dict of guild data with announcement channels"""
        try:
            guilds = self.get_guilds()
            for guild in guilds:
                if server_id and guild['id'] != server_id:
                    continue
                    
                guild_data = {
                    'guild_id': guild['id'],
                    'guild_name': guild['name'],
                    'announcement_channels': {}
                }

                channels = self.get_guild_channels(guild['id'])
                # Ищем announcement каналы в порядке приоритета:
                # 1. Точное название "announcements"
                # 2. Официальный тип 5
                # 3. Другие варианты названий
                announcement_channels = []
                for ch in channels:
                    # 1. Точное совпадение с "announcements"
                    if ch['type'] == 0 and ch['name'].lower() == 'announcements':
                        announcement_channels.append(ch)
                        continue
                    
                    # 2. Официальный тип announcement
                    if ch.get('type') == 5:
                        announcement_channels.append(ch)
                        continue
                    
                    # 3. Другие варианты (только если еще не добавлен)
                    if (ch['type'] == 0 and 
                        any(keyword in ch['name'].lower() 
                            for keyword in ['announce'])):
                        announcement_channels.append(ch)
                
                for channel in announcement_channels:
                    guild_data['announcement_channels'][channel['name']] = {
                        'channel_id': channel['id'],
                        'category': channel.get('parent_id')
                    }

                self.servers_data[guild['name']] = guild_data
                print(f"\nServer: {guild['name']} (ID: {guild['id']})")
                print(f"Announcement Channels: {len(announcement_channels)}")
                for channel in announcement_channels:
                    print(f"  - {channel['name']} (ID: {channel['id']}, Type: {channel.get('type')})")

            # Save data to JSON file
            with open('discord_announcement_channels.json', 'w', encoding='utf-8') as f:
                json.dump(self.servers_data, f, indent=2, ensure_ascii=False)
            
            print("\nData saved to discord_channels.json")
            return self.servers_data
            
        except Exception as e:
            print(f"Error: {str(e)}")

def parse_discord_servers():
    """Get server-channel mappings in format expected by app.py"""
    load_dotenv()
    token = os.getenv('DISCORD_AUTH_TOKENS').strip()
    collector = DiscordIDCollector(token)
    servers_data = collector.collect_ids()
    
    # Transform data to match app.py expected format
    mappings = {}
    for server_name, server_data in servers_data.items():
        mappings[server_name] = {
            channel['channel_id']: channel_name
            for channel_name, channel in server_data['announcement_channels'].items()
        }
    return mappings

# Usage
if __name__ == '__main__':
    load_dotenv()
    token = os.getenv('DISCORD_AUTH_TOKENS').strip()
    print(f"Using token: {token[:10]}...")
    collector = DiscordIDCollector(token)
    collector.collect_ids()
