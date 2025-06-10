#!/usr/bin/env python3
"""
Discord Permissions Diagnostic Tool
This script checks your Discord token permissions and channel access
"""

import requests
import json
import os
from dotenv import load_dotenv
from discord_telegram_parser.config.settings import config

load_dotenv()

class DiscordPermissionsDiagnostic:
    def __init__(self, token):
        self.token = token
        self.session = requests.Session()
        self.session.headers = {'Authorization': token}
        
    def test_token(self):
        """Test if token is valid"""
        print("🔑 Testing Discord Token...")
        try:
            r = self.session.get('https://discord.com/api/v9/users/@me')
            if r.status_code == 200:
                user_info = r.json()
                print(f"✅ Token valid for user: {user_info['username']}#{user_info['discriminator']}")
                print(f"   User ID: {user_info['id']}")
                return True
            else:
                print(f"❌ Token invalid (HTTP {r.status_code}): {r.text}")
                return False
        except Exception as e:
            print(f"❌ Error testing token: {e}")
            return False
    
    def test_guilds_access(self):
        """Test access to guilds"""
        print("\n🏰 Testing Guild Access...")
        try:
            r = self.session.get('https://discord.com/api/v9/users/@me/guilds')
            if r.status_code == 200:
                guilds = r.json()
                print(f"✅ Can access {len(guilds)} guilds")
                
                # Show guilds with announcement channels
                for guild in guilds[:10]:  # Show first 10
                    print(f"   • {guild['name']} (ID: {guild['id']})")
                    
                    # Check if this guild has announcement channels in config
                    if guild['name'] in config.SERVER_CHANNEL_MAPPINGS:
                        channels = config.SERVER_CHANNEL_MAPPINGS[guild['name']]
                        if channels:
                            print(f"     📢 Configured announcement channels: {len(channels)}")
                            for channel_id, channel_name in channels.items():
                                print(f"       - {channel_name} ({channel_id})")
                        else:
                            print(f"     ⚠️ No announcement channels configured")
                    
                if len(guilds) > 10:
                    print(f"   ... and {len(guilds) - 10} more guilds")
                return guilds
            else:
                print(f"❌ Cannot access guilds (HTTP {r.status_code}): {r.text}")
                return []
        except Exception as e:
            print(f"❌ Error accessing guilds: {e}")
            return []
    
    def test_channel_access(self, channel_id, channel_name, server_name):
        """Test access to specific channel"""
        print(f"\n📢 Testing Channel Access: {server_name}#{channel_name}")
        
        # Test 1: Get channel info
        try:
            r = self.session.get(f'https://discord.com/api/v9/channels/{channel_id}')
            if r.status_code == 200:
                channel_info = r.json()
                print(f"✅ Can access channel info")
                print(f"   Channel type: {channel_info.get('type')}")
                print(f"   Guild ID: {channel_info.get('guild_id')}")
                print(f"   Channel name: {channel_info.get('name')}")
            else:
                print(f"❌ Cannot access channel info (HTTP {r.status_code}): {r.text}")
                return False
        except Exception as e:
            print(f"❌ Error getting channel info: {e}")
            return False
        
        # Test 2: Get recent messages
        try:
            r = self.session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
            if r.status_code == 200:
                messages = r.json()
                print(f"✅ Can read messages ({len(messages)} messages retrieved)")
                if messages:
                    msg = messages[0]
                    print(f"   Latest message from: {msg['author']['username']}")
                    print(f"   Content preview: {msg['content'][:50]}...")
                return True
            elif r.status_code == 403:
                print(f"❌ No permission to read messages (HTTP 403)")
                print("   Possible reasons:")
                print("   • Channel is private and you don't have access")
                print("   • Server requires you to have 'Read Message History' permission")
                print("   • Channel has special restrictions")
                return False
            else:
                print(f"❌ Cannot read messages (HTTP {r.status_code}): {r.text}")
                return False
        except Exception as e:
            print(f"❌ Error reading messages: {e}")
            return False
    
    def test_all_configured_channels(self):
        """Test access to all configured announcement channels"""
        print("\n🔍 Testing All Configured Channels...")
        
        accessible_channels = []
        inaccessible_channels = []
        
        for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
            if not channels:
                print(f"⚠️ {server}: No channels configured")
                continue
                
            for channel_id, channel_name in channels.items():
                print(f"\n   Testing {server}#{channel_name} ({channel_id})...")
                if self.test_channel_access_quick(channel_id, channel_name, server):
                    accessible_channels.append((server, channel_name, channel_id))
                    print(f"   ✅ {server}#{channel_name} - Accessible")
                else:
                    inaccessible_channels.append((server, channel_name, channel_id))
                    print(f"   ❌ {server}#{channel_name} - Not accessible")
        
        print(f"\n📊 Summary:")
        print(f"✅ Accessible channels: {len(accessible_channels)}")
        print(f"❌ Inaccessible channels: {len(inaccessible_channels)}")
        
        if accessible_channels:
            print(f"\n✅ Working channels:")
            for server, channel_name, channel_id in accessible_channels:
                print(f"   • {server}#{channel_name}")
        
        if inaccessible_channels:
            print(f"\n❌ Problematic channels:")
            for server, channel_name, channel_id in inaccessible_channels:
                print(f"   • {server}#{channel_name} - Check permissions")
        
        return accessible_channels, inaccessible_channels
    
    def test_channel_access_quick(self, channel_id, channel_name, server_name):
        """Quick test for channel access"""
        try:
            r = self.session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
            return r.status_code == 200
        except:
            return False
    
    def suggest_fixes(self, inaccessible_channels):
        """Suggest fixes for permission issues"""
        if not inaccessible_channels:
            print("\n🎉 All channels are accessible!")
            return
            
        print(f"\n🔧 Suggested Fixes for {len(inaccessible_channels)} inaccessible channels:")
        print(f"\n1. Check Discord Server Permissions:")
        print(f"   • Make sure you have 'Read Messages' permission in the server")
        print(f"   • Make sure you have 'Read Message History' permission")
        print(f"   • Some servers require you to have a specific role")
        
        print(f"\n2. Channel-Specific Issues:")
        print(f"   • Some announcement channels might be private/restricted")
        print(f"   • Channel might have been deleted or renamed")
        print(f"   • You might have been removed from the server")
        
        print(f"\n3. Token Issues:")
        print(f"   • Try logging out and back into Discord")
        print(f"   • Generate a new token from Discord Developer Portal")
        print(f"   • Make sure your account isn't restricted")
        
        print(f"\n4. Update Configuration:")
        print(f"   • Remove inaccessible channels from config")
        print(f"   • Re-run channel discovery to find new channels")
    
    def run_full_diagnostic(self):
        """Run complete diagnostic"""
        print("🚀 Discord Permissions Diagnostic Tool")
        print("=" * 50)
        
        # Test 1: Token validity
        if not self.test_token():
            print("\n❌ Token test failed. Please check your Discord token.")
            return False
        
        # Test 2: Guild access
        guilds = self.test_guilds_access()
        if not guilds:
            print("\n❌ Cannot access any guilds. Please check token permissions.")
            return False
        
        # Test 3: Channel access
        accessible, inaccessible = self.test_all_configured_channels()
        
        # Test 4: Suggest fixes
        self.suggest_fixes(inaccessible)
        
        print(f"\n📋 Diagnostic Complete!")
        print(f"   Working channels: {len(accessible)}")
        print(f"   Problem channels: {len(inaccessible)}")
        
        return len(accessible) > 0

def main():
    """Run diagnostic with your current token"""
    token = os.getenv('DISCORD_AUTH_TOKENS', '').split(',')[0].strip()
    
    if not token:
        print("❌ No Discord token found in .env file")
        return
    
    print(f"🔑 Using token: {token[:20]}...")
    diagnostic = DiscordPermissionsDiagnostic(token)
    diagnostic.run_full_diagnostic()

if __name__ == '__main__':
    main()