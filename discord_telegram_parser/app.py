import json
import os
import asyncio
import threading
import time
from loguru import logger
from datetime import datetime, timedelta

from discord_telegram_parser.services.telegram_bot import TelegramBotService
from discord_telegram_parser.services.discord_websocket import DiscordWebSocketService
from discord_telegram_parser.config.settings import config
from discord_telegram_parser.main import DiscordParser

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
        
        # Новые атрибуты для управления новыми серверами
        self.new_server_handler = NewServerHandler(self.telegram_bot, self.discord_parser)
        
    def discover_channels(self):
        """Discover announcement channels using channel_id_parser"""
        from discord_telegram_parser.utils.channel_id_parser import parse_discord_servers
        
        mappings = parse_discord_servers()
        if mappings:
            # Проверяем на новые серверы
            old_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys()) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else set()
            new_servers = set(mappings.keys()) - old_servers
            
            if new_servers:
                logger.info(f"🆕 Discovered {len(new_servers)} new servers:")
                for server in new_servers:
                    logger.info(f"   • {server}")
                    
                    # Планируем отложенную обработку для новых серверов
                    if self.new_server_handler:
                        self.new_server_handler.schedule_new_server_processing(server, mappings[server])
            
            config.SERVER_CHANNEL_MAPPINGS = mappings
            
            # Add discovered channels to WebSocket subscriptions
            for server, channels in mappings.items():
                for channel_id in channels.keys():
                    self.websocket_service.add_channel_subscription(channel_id)
        else:
            logger.warning("Failed to discover channels")
    
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
            if isinstance(text, str):
                text = text.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                text = ''.join(char for char in text if ord(char) < 0x110000)
            return text
        except (UnicodeEncodeError, UnicodeDecodeError):
            return "[Encoding Error]"
    
    def test_channel_http_access(self, channel_id):
        """Quick test if channel is accessible via HTTP"""
        try:
            session = self.discord_parser.sessions[0]
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
            
            # Выполняем проверку топиков при запуске для предотвращения дублей
            if not self.telegram_bot.startup_verification_done:
                logger.info("🔍 Performing startup verification to prevent topic duplicates...")
                self.telegram_bot.startup_topic_verification()
            
            # Clean up invalid topics
            cleaned_topics = self.telegram_bot.cleanup_invalid_topics()
            if cleaned_topics > 0:
                logger.info(f"   🧹 Cleaned {cleaned_topics} invalid/duplicate topics")
                telegram_topics = set(self.telegram_bot.server_topics.keys())
            
            # Find new servers (don't create topics yet - wait for verification)
            new_servers = current_servers - telegram_topics
            if new_servers:
                logger.info(f"   🆕 New servers found: {len(new_servers)}")
                for server in new_servers:
                    logger.info(f"      • {server} (topic will be created after verification)")
            
            # Find removed servers to delete topics
            removed_servers = telegram_topics - current_servers
            if removed_servers:
                logger.info(f"   🗑️ Removing topics for deleted servers: {len(removed_servers)}")
                for server in removed_servers:
                    if server in self.telegram_bot.server_topics:
                        old_topic_id = self.telegram_bot.server_topics[server]
                        del self.telegram_bot.server_topics[server]
                        if old_topic_id in self.telegram_bot.topic_name_cache:
                            del self.telegram_bot.topic_name_cache[old_topic_id]
                        logger.info(f"      • Removed {server} (topic {old_topic_id})")
                
                if removed_servers:
                    self.telegram_bot._save_data()
            
            logger.success(f"✅ Server sync completed with anti-duplicate protection")
            
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"❌ Error in server sync: {error_msg}")

    def initial_sync(self):
        """Perform initial sync with improved topic management and verification delay"""
        try:
            # Discover channels if not already configured
            if not config.SERVER_CHANNEL_MAPPINGS:
                self.discover_channels()
            
            # Sync servers between Discord and Telegram with duplicate prevention
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
                
                # Проверяем, новый ли это сервер (может требовать верификации)
                is_new_server = server not in self.telegram_bot.server_topics
                if is_new_server:
                    logger.info(f"🆕 New server detected: {safe_server} - will process after verification")
                    # Планируем отложенную обработку
                    self.new_server_handler.schedule_new_server_processing(server, channels)
                    continue
                    
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
            logger.info(f"   🆕 New servers: {len(self.new_server_handler.pending_servers)} (processing after verification)")
            
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
                
                logger.info(f"📤 Sending messages for {len(server_messages)} servers with anti-duplicate topic logic")
                
                # Send messages with proper topic management (no duplicates!)
                for server, msgs in server_messages.items():
                    logger.info(f"   📍 {server}: {len(msgs)} messages")
                    # This will use the improved topic logic (one server = one topic, no duplicates)
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
                
                server_messages = {}
                recent_threshold = datetime.now().timestamp() - 120  # 2 minutes ago
                
                for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                    # Пропускаем новые серверы, которые еще не прошли верификацию
                    if server in self.new_server_handler.pending_servers:
                        continue
                        
                    safe_server = self.safe_encode_string(server)
                    
                    for channel_id, channel_name in channels.items():
                        # Only poll HTTP-accessible channels
                        if not self.test_channel_http_access(channel_id):
                            continue
                            
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
                
                # Send messages grouped by server (proper topic management with no duplicates)
                if server_messages:
                    total_messages = sum(len(msgs) for msgs in server_messages.values())
                    logger.info(f"🔄 Fallback polling found {total_messages} new messages in {len(server_messages)} servers")
                    
                    for server, msgs in server_messages.items():
                        msgs.sort(key=lambda x: x.timestamp)
                        logger.info(f"   📍 {server}: {len(msgs)} messages")
                        self.telegram_bot.send_messages(msgs)  # Uses improved topic logic with duplicate prevention
                
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"Error in fallback polling: {error_msg}")
                time.sleep(60)
    
    def run(self):
        """Run all components with improved topic management and verification handling"""
        self.running = True
        
        try:
            # Perform smart initial sync with improved topic logic and verification handling
            logger.info("🚀 Starting smart initial sync with enhanced anti-duplicate topic management...")
            self.initial_sync()
            
            # Start Telegram bot in separate thread
            bot_thread = threading.Thread(
                target=self.telegram_bot.start_bot,
                daemon=True
            )
            bot_thread.start()
            logger.success("✅ Telegram bot started with enhanced anti-duplicate topic logic")
            
            # Start WebSocket service in separate thread
            websocket_thread = self.run_websocket_in_thread()
            logger.success("✅ WebSocket service started with new server detection")
            
            # Start fallback polling in separate thread (HTTP channels only)
            fallback_thread = threading.Thread(
                target=self.fallback_polling_loop,
                daemon=True
            )
            fallback_thread.start()
            logger.success("✅ Fallback polling started (HTTP channels only)")
            
            # Start new server handler
            new_server_thread = threading.Thread(
                target=self.new_server_handler.run,
                daemon=True
            )
            new_server_thread.start()
            logger.success("✅ New server handler started with verification delay")
            
            # Keep main thread alive
            logger.success("🎉 Discord Telegram Parser running with ENHANCED features!")
            logger.info("📊 Enhanced Features:")
            logger.info("   🛡️ ANTI-DUPLICATE topic protection (startup verification)")
            logger.info("   ⏰ New server verification delay (3 minutes)")
            logger.info("   🔒 Thread-safe topic creation")
            logger.info("   🧹 Auto-cleanup of invalid/duplicate topics")
            logger.info("   📡 HTTP channels: Initial sync + fallback polling")
            logger.info("   🔌 WebSocket channels: Real-time monitoring")
            logger.info("   🆕 Automatic new server detection and setup")
            logger.info("   📋 Messages grouped by server (one topic per server)")
            logger.info("Press Ctrl+C to stop")
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.running = False
            
            # Stop WebSocket service
            if self.websocket_service:
                asyncio.run(self.websocket_service.stop())
                
            # Stop new server handler
            if self.new_server_handler:
                self.new_server_handler.stop()
                
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"Error in main run loop: {error_msg}")
            self.running = False


class NewServerHandler:
    """Обработчик новых серверов с задержкой для верификации"""
    
    def __init__(self, telegram_bot, discord_parser):
        self.telegram_bot = telegram_bot
        self.discord_parser = discord_parser
        self.pending_servers = {}  # server_name -> {'channels': {}, 'join_time': datetime}
        self.verification_delay = 180  # 3 минуты
        self.running = False
        
    def schedule_new_server_processing(self, server_name, channels):
        """Планирует обработку нового сервера с задержкой"""
        logger.info(f"📅 Scheduling new server processing: {server_name}")
        logger.info(f"⏰ Will process after {self.verification_delay} seconds for verification")
        
        self.pending_servers[server_name] = {
            'channels': channels,
            'join_time': datetime.now(),
            'processed': False
        }
        
        logger.info(f"📋 Pending servers: {len(self.pending_servers)}")
    
    def check_server_verification(self, server_name, channels):
        """Проверяет, прошел ли сервер верификацию досрочно"""
        try:
            # Пытаемся получить доступ к одному из каналов
            for channel_id in list(channels.keys())[:1]:  # Проверяем только первый канал
                try:
                    session = self.discord_parser.sessions[0]
                    r = session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
                    if r.status_code == 200:
                        logger.info(f"✅ Early verification passed for {server_name}")
                        return True
                    elif r.status_code == 403:
                        # Все еще нет доступа
                        return False
                except Exception as e:
                    logger.debug(f"Verification check error for {server_name}: {e}")
                    continue
                    
        except Exception as e:
            logger.debug(f"Server verification check failed for {server_name}: {e}")
            
        return False
    
    def process_new_server(self, server_name, channels):
        """Обрабатывает новый сервер: создает топик и отправляет сообщения"""
        try:
            logger.info(f"🚀 Processing new server: {server_name}")
            
            # Добавляем сервер в конфигурацию, если его еще нет
            if server_name not in config.SERVER_CHANNEL_MAPPINGS:
                config.SERVER_CHANNEL_MAPPINGS[server_name] = channels
                logger.info(f"📝 Added {server_name} to configuration with {len(channels)} channels")
            
            # Создаем топик для нового сервера (с защитой от дублей)
            topic_id = self.telegram_bot._get_or_create_topic_safe(server_name)
            
            if not topic_id:
                logger.error(f"❌ Failed to create topic for {server_name}")
                return False
            
            logger.success(f"✅ Created topic {topic_id} for new server: {server_name}")
            
            # Собираем последние сообщения из всех каналов нового сервера
            all_messages = []
            accessible_channels = 0
            
            for channel_id, channel_name in channels.items():
                try:
                    # Проверяем доступ к каналу
                    session = self.discord_parser.sessions[0]
                    test_response = session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
                    
                    if test_response.status_code == 200:
                        accessible_channels += 1
                        
                        # Получаем последние сообщения
                        messages = self.discord_parser.parse_announcement_channel(
                            channel_id,
                            server_name,
                            channel_name,
                            limit=5  # По 5 сообщений с каждого канала
                        )
                        
                        if messages:
                            all_messages.extend(messages)
                            logger.info(f"📥 Collected {len(messages)} messages from #{channel_name}")
                        
                    elif test_response.status_code == 403:
                        logger.warning(f"⚠️ Still no access to {server_name}#{channel_name} - may need more time")
                    else:
                        logger.warning(f"⚠️ Unexpected response {test_response.status_code} for {server_name}#{channel_name}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error accessing {server_name}#{channel_name}: {e}")
                    continue
            
            logger.info(f"📊 Server {server_name} access summary:")
            logger.info(f"   📡 Total channels: {len(channels)}")
            logger.info(f"   ✅ Accessible channels: {accessible_channels}")
            logger.info(f"   📨 Total messages collected: {len(all_messages)}")
            
            # Отправляем приветственное сообщение
            welcome_msg = (
                f"🎉 **Welcome to {server_name}!**\n\n"
                f"🆕 This server was just added to monitoring.\n"
                f"📡 Monitoring {accessible_channels}/{len(channels)} channels.\n"
                f"📨 Found {len(all_messages)} recent announcements.\n\n"
            )
            
            if accessible_channels == 0:
                welcome_msg += (
                    "⚠️ **No channels accessible yet** - this is normal for new servers.\n"
                    "🔒 Please complete server verification/rules acceptance.\n"
                    "📡 Will start monitoring once access is granted.\n\n"
                    "🔄 You can manually check later using /servers command."
                )
            elif accessible_channels < len(channels):
                welcome_msg += (
                    f"📋 **Latest announcements** (from accessible channels):"
                )
            else:
                welcome_msg += (
                    f"📋 **Latest announcements** from this server:"
                )
            
            # Отправляем приветственное сообщение
            sent_welcome = self.telegram_bot._send_message(
                welcome_msg,
                message_thread_id=topic_id,
                server_name=server_name
            )
            
            if sent_welcome:
                logger.success(f"✅ Sent welcome message to {server_name}")
            
            # Отправляем собранные сообщения, если есть
            if all_messages:
                # Сортируем по времени и берем последние 10
                all_messages.sort(key=lambda x: x.timestamp)
                recent_messages = all_messages[-10:]
                
                logger.info(f"📤 Sending {len(recent_messages)} recent messages to {server_name}")
                self.telegram_bot.send_messages(recent_messages)
                
            else:
                # Отправляем информационное сообщение
                info_msg = (
                    "ℹ️ No recent messages found - this is normal for new servers.\n"
                    "📡 Real-time monitoring is now active!\n"
                    "🔔 You'll receive new announcements as they're posted."
                )
                
                self.telegram_bot._send_message(
                    info_msg,
                    message_thread_id=topic_id,
                    server_name=server_name
                )
            
            logger.success(f"🎉 Successfully set up new server: {server_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing new server {server_name}: {e}")
            return False
    
    def run(self):
        """Основной цикл обработки новых серверов"""
        self.running = True
        logger.info("🚀 New Server Handler started")
        
        while self.running:
            try:
                time.sleep(30)  # Проверяем каждые 30 секунд
                
                current_time = datetime.now()
                servers_to_process = []
                
                # Проверяем pending серверы
                for server_name, server_data in list(self.pending_servers.items()):
                    if server_data['processed']:
                        continue
                        
                    join_time = server_data['join_time']
                    channels = server_data['channels']
                    
                    # Проверяем, прошло ли достаточно времени или получили ли мы доступ досрочно
                    time_elapsed = (current_time - join_time).total_seconds()
                    
                    if time_elapsed >= self.verification_delay:
                        # Время вышло, обрабатываем в любом случае
                        logger.info(f"⏰ Verification time elapsed for {server_name}, processing now")
                        servers_to_process.append(server_name)
                        
                    elif time_elapsed >= 60:  # Проверяем досрочную верификацию только после 1 минуты
                        # Проверяем досрочную верификацию
                        if self.check_server_verification(server_name, channels):
                            logger.info(f"✅ Early verification detected for {server_name}")
                            servers_to_process.append(server_name)
                
                # Обрабатываем серверы
                for server_name in servers_to_process:
                    if server_name in self.pending_servers:
                        server_data = self.pending_servers[server_name]
                        
                        if self.process_new_server(server_name, server_data['channels']):
                            # Помечаем как обработанный
                            self.pending_servers[server_name]['processed'] = True
                            logger.success(f"✅ Successfully processed new server: {server_name}")
                        else:
                            logger.error(f"❌ Failed to process new server: {server_name}")
                
                # Удаляем обработанные серверы старше 1 часа
                cutoff_time = current_time - timedelta(hours=1)
                for server_name in list(self.pending_servers.keys()):
                    server_data = self.pending_servers[server_name]
                    if (server_data['processed'] and 
                        server_data['join_time'] < cutoff_time):
                        del self.pending_servers[server_name]
                        logger.debug(f"🧹 Cleaned up processed server: {server_name}")
                
            except Exception as e:
                logger.error(f"❌ Error in new server handler: {e}")
                time.sleep(60)  # Ждем дольше при ошибке
    
    def stop(self):
        """Остановка обработчика новых серверов"""
        self.running = False
        logger.info("🛑 New Server Handler stopped")


def main():
    """Main entry point for the application"""
    logger.info("🚀 Starting Discord Telegram Parser with ENHANCED features...")
    logger.info("✨ Features enabled:")
    logger.info("   🛡️ Anti-duplicate topic protection")
    logger.info("   ⏰ New server verification delay")
    logger.info("   🔍 Startup topic verification")
    logger.info("   🆕 Automatic new server setup")
    logger.info("   📡 Real-time WebSocket monitoring")
    
    app = DiscordTelegramParser()
    app.run()

if __name__ == '__main__':
    main()