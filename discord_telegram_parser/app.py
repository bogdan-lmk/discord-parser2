import json
import os
import asyncio
import threading
import time
from loguru import logger
from datetime import datetime

from .services.telegram_bot import TelegramBotService
from .services.discord_websocket import DiscordWebSocketService
from .config.settings import config
from .main import DiscordParser

class DiscordTelegramParser:
    def __init__(self):
        # Reload environment variables before initializing services
        from dotenv import load_dotenv
        load_dotenv(override=True)
        
        self.discord_parser = DiscordParser()
        self.telegram_bot = TelegramBotService(config.TELEGRAM_BOT_TOKEN)
        self.websocket_service = DiscordWebSocketService(self.telegram_bot)
        
        # Cross-reference services
        self.telegram_bot.discord_parser = self.discord_parser
        self.telegram_bot.websocket_service = self.websocket_service
        
        self.running = False
        self.websocket_task = None
        
    def discover_channels(self):
        """Discover announcement channels using channel_id_parser"""
        from discord_telegram_parser.utils.channel_id_parser import parse_discord_servers
        
        mappings = parse_discord_servers()
        if mappings:
            config.SERVER_CHANNEL_MAPPINGS = mappings
            
            # Add discovered channels to WebSocket subscriptions
            for server, channels in mappings.items():
                for channel_id in channels.keys():
                    self.websocket_service.add_channel_subscription(channel_id)
            
            # Save discovered channels to config file
            with open('discord_telegram_parser/config/settings.py', 'a') as f:
                f.write(f"\n# Auto-discovered channels\nconfig.SERVER_CHANNEL_MAPPINGS = {json.dumps(mappings, indent=2)}\n")
        else:
            print("Failed to discover channels")
    
    async def websocket_main_loop(self):
        """Main async loop for WebSocket service"""
        while self.running:
            try:
                logger.info("Starting WebSocket connections...")
                await self.websocket_service.start()
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"WebSocket error: {error_msg}")
                logger.info("Restarting WebSocket in 30 seconds...")
                await asyncio.sleep(30)
    
    def run_websocket_in_thread(self):
        """Run WebSocket service in separate thread with async loop"""
        def websocket_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.websocket_main_loop())
            except Exception as e:
                logger.error(f"WebSocket thread error: {e}")
            finally:
                loop.close()
        
        thread = threading.Thread(target=websocket_thread, daemon=True)
        thread.start()
        return thread
    
    def safe_encode_string(self, text):
        """Safely encode string to handle Unicode issues"""
        if not text:
            return ""
        try:
            # Handle surrogates and problematic characters
            if isinstance(text, str):
                # Remove surrogates and invalid characters
                text = text.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                # Filter out characters that might cause issues
                text = ''.join(char for char in text if ord(char) < 0x110000)
            return text
        except (UnicodeEncodeError, UnicodeDecodeError):
            return "[Encoding Error]"
    
    def test_channel_http_access(self, channel_id):
        """Quick test if channel is accessible via HTTP"""
        try:
            session = self.discord_parser.sessions[0]  # Use first session
            r = session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
            return r.status_code == 200
        except:
            return False
    
    def sync_servers(self):
        """Sync Discord servers with Telegram topics - improved version"""
        try:
            # Get current Discord servers
            current_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys())
            
            # Get Telegram topics
            telegram_topics = set(self.telegram_bot.server_topics.keys())
            
            logger.info(f"🔄 Syncing servers...")
            logger.info(f"   Discord servers: {len(current_servers)}")
            logger.info(f"   Telegram topics: {len(telegram_topics)}")
            
            # Clean up invalid topics first
            cleaned_topics = self.telegram_bot.cleanup_invalid_topics()
            if cleaned_topics > 0:
                logger.info(f"   🧹 Cleaned {cleaned_topics} invalid topics")
                telegram_topics = set(self.telegram_bot.server_topics.keys())  # Refresh after cleanup
            
            # Find new servers (don't create topics yet - wait for actual messages)
            new_servers = current_servers - telegram_topics
            if new_servers:
                logger.info(f"   🆕 New servers found: {len(new_servers)}")
                for server in new_servers:
                    logger.info(f"      • {server} (topic will be created when needed)")
            
            # Find removed servers to delete topics
            removed_servers = telegram_topics - current_servers
            if removed_servers:
                logger.info(f"   🗑️ Removing topics for deleted servers: {len(removed_servers)}")
                for server in removed_servers:
                    if server in self.telegram_bot.server_topics:
                        old_topic_id = self.telegram_bot.server_topics[server]
                        del self.telegram_bot.server_topics[server]
                        logger.info(f"      • Removed {server} (topic {old_topic_id})")
                
                if removed_servers:
                    self.telegram_bot._save_data()
            
            logger.success(f"✅ Server sync completed")
            
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"❌ Error in server sync: {error_msg}")

    def initial_sync(self):
        """Perform initial sync with improved topic management"""
        try:
            # Discover channels if not already configured
            if not config.SERVER_CHANNEL_MAPPINGS:
                self.discover_channels()
            
            # Sync servers between Discord and Telegram (cleanup invalid topics)
            self.sync_servers()
            
            # Get recent messages from HTTP-accessible channels only
            logger.info("🔍 Performing smart initial sync (HTTP-accessible channels only)...")
            messages = []
            http_channels = []
            websocket_only_channels = []
            
            for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                if not channels:
                    continue
                
                safe_server = self.safe_encode_string(server)
                    
                for channel_id, channel_name in channels.items():
                    safe_channel = self.safe_encode_string(channel_name)
                    
                    # Quick HTTP access test
                    if self.test_channel_http_access(channel_id):
                        # HTTP accessible - sync
                        try:
                            recent_messages = self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                safe_server,
                                safe_channel,
                                limit=5
                            )
                            
                            # Clean message content for encoding issues
                            for msg in recent_messages:
                                msg.content = self.safe_encode_string(msg.content)
                                msg.author = self.safe_encode_string(msg.author)
                                msg.server_name = self.safe_encode_string(msg.server_name)
                                msg.channel_name = self.safe_encode_string(msg.channel_name)
                            
                            messages.extend(recent_messages)
                            http_channels.append((safe_server, safe_channel))
                            logger.info(f"✅ HTTP sync: {safe_server}#{safe_channel} - {len(recent_messages)} messages")
                            
                        except Exception as channel_error:
                            safe_error = str(channel_error).encode('utf-8', 'replace').decode('utf-8')
                            logger.warning(f"❌ HTTP sync failed: {safe_server}#{safe_channel}: {safe_error}")
                            websocket_only_channels.append((safe_server, safe_channel))
                    else:
                        # HTTP not accessible - leave for WebSocket
                        websocket_only_channels.append((safe_server, safe_channel))
                        logger.info(f"🔌 WebSocket only: {safe_server}#{safe_channel} - will monitor via WebSocket")
            
            # Summary
            logger.info(f"📊 Initial sync summary:")
            logger.info(f"   ✅ HTTP synced: {len(http_channels)} channels")
            logger.info(f"   🔌 WebSocket only: {len(websocket_only_channels)} channels")
            logger.info(f"   📨 Total messages: {len(messages)}")
            
            if websocket_only_channels:
                logger.info(f"🔌 These channels will be monitored via WebSocket only:")
                for server, channel in websocket_only_channels:
                    logger.info(f"   • {server}#{channel}")
            
            # Group messages by server before sending to Telegram
            if messages:
                messages.sort(key=lambda x: x.timestamp)
                
                # Group by server to ensure proper topic management
                server_messages = {}
                for msg in messages:
                    server = msg.server_name
                    if server not in server_messages:
                        server_messages[server] = []
                    server_messages[server].append(msg)
                
                logger.info(f"📤 Sending messages for {len(server_messages)} servers with improved topic logic")
                
                # Send messages with proper topic management
                for server, msgs in server_messages.items():
                    logger.info(f"   📍 {server}: {len(msgs)} messages")
                    # This will use the improved topic logic (one server = one topic)
                    self.telegram_bot.send_messages(msgs)
                
                logger.success(f"✅ Initial HTTP sync completed: {len(messages)} messages sent")
            else:
                logger.info("ℹ️ No HTTP messages found during initial sync")
            
            logger.success(f"🎉 Smart initial sync complete! WebSocket will handle real-time monitoring.")
            
        except Exception as e:
            try:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            except:
                error_msg = "Initial sync error (encoding issue)"
            logger.error(f"❌ Error in initial sync: {error_msg}")
    
    def fallback_polling_loop(self):
        """Improved fallback polling with proper topic management"""
        while self.running:
            try:
                time.sleep(300)  # Check every 5 minutes as fallback
                
                if not config.SERVER_CHANNEL_MAPPINGS:
                    continue
                
                logger.debug("🔄 Fallback polling check (HTTP channels only)...")
                
                server_messages = {}  # Group by server
                recent_threshold = datetime.now().timestamp() - 120  # 2 minutes ago
                
                for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                    safe_server = self.safe_encode_string(server)
                    
                    for channel_id, channel_name in channels.items():
                        # Only poll HTTP-accessible channels
                        if not self.test_channel_http_access(channel_id):
                            continue  # Skip WebSocket-only channels
                            
                        try:
                            safe_channel = self.safe_encode_string(channel_name)
                            
                            recent_messages = self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                safe_server,
                                safe_channel,
                                limit=3
                            )
                            
                            # Clean message content for encoding issues
                            for msg in recent_messages:
                                msg.content = self.safe_encode_string(msg.content)
                                msg.author = self.safe_encode_string(msg.author)
                                msg.server_name = self.safe_encode_string(msg.server_name)
                                msg.channel_name = self.safe_encode_string(msg.channel_name)
                            
                            # Filter for very recent messages
                            new_messages = [
                                msg for msg in recent_messages
                                if msg.timestamp.timestamp() > recent_threshold
                            ]
                            
                            # Group by server
                            if new_messages:
                                if safe_server not in server_messages:
                                    server_messages[safe_server] = []
                                server_messages[safe_server].extend(new_messages)
                            
                        except Exception as e:
                            logger.debug(f"Fallback polling error for {safe_server}#{safe_channel}: {e}")
                            continue
                
                # Send messages grouped by server (proper topic management)
                if server_messages:
                    total_messages = sum(len(msgs) for msgs in server_messages.values())
                    logger.info(f"🔄 Fallback polling found {total_messages} new messages in {len(server_messages)} servers")
                    
                    for server, msgs in server_messages.items():
                        msgs.sort(key=lambda x: x.timestamp)  # Chronological order
                        logger.info(f"   📍 {server}: {len(msgs)} messages")
                        self.telegram_bot.send_messages(msgs)  # Uses improved topic logic
                
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"Error in fallback polling: {error_msg}")
                time.sleep(60)
    
    def run(self):
        """Run all components with improved topic management"""
        self.running = True
        
        try:
            # Perform smart initial sync with improved topic logic
            logger.info("🚀 Starting smart initial sync with improved topic management...")
            self.initial_sync()
            
            # Start Telegram bot in separate thread
            bot_thread = threading.Thread(
                target=self.telegram_bot.start_bot,
                daemon=True
            )
            bot_thread.start()
            logger.success("✅ Telegram bot started with improved topic logic")
            
            # Start WebSocket service in separate thread
            websocket_thread = self.run_websocket_in_thread()
            logger.success("✅ WebSocket service started")
            
            # Start fallback polling in separate thread (HTTP channels only)
            fallback_thread = threading.Thread(
                target=self.fallback_polling_loop,
                daemon=True
            )
            fallback_thread.start()
            logger.success("✅ Fallback polling started (HTTP channels only)")
            
            # Keep main thread alive
            logger.success("🎉 Discord Telegram Parser running with improved topic management!")
            logger.info("📊 Features:")
            logger.info("   ✅ One server = One topic (no duplicates)")
            logger.info("   ✅ Thread-safe topic creation")
            logger.info("   ✅ Auto-cleanup of invalid topics")
            logger.info("   ✅ HTTP channels: Initial sync + fallback polling")
            logger.info("   ✅ WebSocket channels: Real-time monitoring")
            logger.info("   ✅ Messages grouped by server")
            logger.info("Press Ctrl+C to stop")
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.running = False
            
            # Stop WebSocket service
            if self.websocket_service:
                asyncio.run(self.websocket_service.stop())
                
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"Error in main run loop: {error_msg}")
            self.running = False

def main():
    """Main entry point for the application"""
    logger.info("Starting Discord Telegram Parser with WebSocket support...")
    app = DiscordTelegramParser()
    app.run()

if __name__ == '__main__':
    main()
