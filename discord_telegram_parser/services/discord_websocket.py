import asyncio
import json
import aiohttp
import time
from datetime import datetime
from loguru import logger
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config

class DiscordWebSocketService:
    def __init__(self, telegram_bot=None):
        self.telegram_bot = telegram_bot
        self.websockets = []
        self.heartbeat_interval = 41250
        self.session_id = None
        self.last_sequence = None
        self.subscribed_channels = set()
        self.http_accessible_channels = set()  # Каналы доступные через HTTP
        self.websocket_accessible_channels = set()  # Каналы доступные через WebSocket
        self.running = False
        
        # Initialize WebSocket sessions for each token
        for token in config.DISCORD_TOKENS:
            ws_session = {
                'token': token,
                'websocket': None,
                'session': None,
                'heartbeat_task': None,
                'user_id': None
            }
            self.websockets.append(ws_session)
    
    async def identify(self, websocket, token):
        """Send IDENTIFY payload with comprehensive intents"""
        identify_payload = {
            "op": 2,
            "d": {
                "token": token,
                "properties": {
                    "$os": "linux",
                    "$browser": "discord_parser",
                    "$device": "discord_parser"
                },
                "compress": False,
                "large_threshold": 50,
                "intents": 33281  # GUILDS (1) + GUILD_MESSAGES (512) + MESSAGE_CONTENT (32768)
            }
        }
        await websocket.send_str(json.dumps(identify_payload))
        logger.info("🔑 Sent IDENTIFY with comprehensive intents (33281)")
    
    async def send_heartbeat(self, websocket, interval):
        """Send periodic heartbeat to maintain connection"""
        try:
            while self.running:
                heartbeat_payload = {
                    "op": 1,
                    "d": self.last_sequence
                }
                await websocket.send_str(json.dumps(heartbeat_payload))
                logger.debug("💓 Sent heartbeat")
                await asyncio.sleep(interval / 1000)
        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled")
        except Exception as e:
            logger.error(f"Error in heartbeat: {e}")
    
    async def test_http_access(self, channel_id, server_name, channel_name, token):
        """Test HTTP API access"""
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': token}
                
                async with session.get(
                    f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1',
                    headers=headers
                ) as resp:
                    if resp.status == 200:
                        return True
                    else:
                        return False
                        
        except Exception as e:
            return False
    
    def check_websocket_channel_access(self, channel_id, guilds_data):
        """Check if channel is accessible via WebSocket guild data"""
        for guild in guilds_data:
            channels = guild.get('channels', [])
            for channel in channels:
                if channel['id'] == channel_id:
                    # Проверяем в порядке приоритета:
                    # 1. Точное название "announcements"
                    if channel['type'] == 0 and channel['name'].lower() == 'announcements':
                        return True
                    # 2. Официальный тип announcement
                    if channel.get('type') == 5:
                        return True
                    # 3. Другие варианты названий
                    if (channel['type'] == 0 and 
                        any(keyword in channel['name'].lower() 
                            for keyword in ['announcements'])):
                        return True
        return False
    
    async def handle_gateway_message(self, data, ws_session):
        """Handle incoming WebSocket messages from Discord Gateway"""
        try:
            if data['op'] == 10:  # HELLO
                self.heartbeat_interval = data['d']['heartbeat_interval']
                logger.info(f"👋 Received HELLO, heartbeat interval: {self.heartbeat_interval}ms")
                
                # Start heartbeat
                ws_session['heartbeat_task'] = asyncio.create_task(
                    self.send_heartbeat(ws_session['websocket'], self.heartbeat_interval)
                )
                
                # Send IDENTIFY
                await self.identify(ws_session['websocket'], ws_session['token'])
                
            elif data['op'] == 11:  # HEARTBEAT_ACK
                logger.debug("💚 Received heartbeat ACK")
                
            elif data['op'] == 0:  # DISPATCH
                self.last_sequence = data['s']
                event_type = data['t']
                
                if event_type == 'READY':
                    self.session_id = data['d']['session_id']
                    ws_session['user_id'] = data['d']['user']['id']
                    user = data['d']['user']
                    guilds = data['d']['guilds']
                    
                    logger.success(f"🚀 WebSocket ready for user: {user['username']}")
                    logger.info(f"🏰 Connected to {len(guilds)} guilds")
                    
                    # Используем гибридный подход для верификации каналов
                    await self.hybrid_channel_verification(ws_session, guilds)
                    
                elif event_type == 'MESSAGE_CREATE':
                    await self.handle_new_message(data['d'])
                    
                elif event_type == 'GUILD_CREATE':
                    guild = data['d']
                    logger.info(f"🏰 Guild loaded: {guild['name']} ({guild['id']})")
                    await self.process_guild_channels(guild, ws_session)
                    
        except Exception as e:
            logger.error(f"Error handling gateway message: {e}")
    
    async def hybrid_channel_verification(self, ws_session, guilds_data):
        """Hybrid verification: HTTP + WebSocket channel discovery"""
        logger.info("🔍 Starting hybrid channel verification...")
        
        http_working = []
        websocket_only = []
        total_monitoring = []
        failed_completely = []
        
        # Проверяем все каналы из конфига
        for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
            if not channels:
                continue
                
            for channel_id, channel_name in channels.items():
                logger.info(f"🧪 Testing {server}#{channel_name}...")
                
                # Тест 1: HTTP API
                http_works = await self.test_http_access(
                    channel_id, server, channel_name, ws_session['token']
                )
                
                # Тест 2: WebSocket (проверяем наличие в guild data)
                websocket_works = self.check_websocket_channel_access(channel_id, guilds_data)
                
                if http_works and websocket_works:
                    # Оба метода работают - идеально!
                    self.http_accessible_channels.add(channel_id)
                    self.websocket_accessible_channels.add(channel_id)
                    self.subscribed_channels.add(channel_id)
                    http_working.append((server, channel_name, channel_id))
                    total_monitoring.append((server, channel_name, channel_id, "HTTP+WS"))
                    logger.success(f"   ✅ {server}#{channel_name} - Both HTTP & WebSocket work")
                    
                elif not http_works and websocket_works:
                    # Только WebSocket работает
                    self.websocket_accessible_channels.add(channel_id)
                    self.subscribed_channels.add(channel_id)
                    websocket_only.append((server, channel_name, channel_id))
                    total_monitoring.append((server, channel_name, channel_id, "WS only"))
                    logger.warning(f"   🤔 {server}#{channel_name} - WebSocket only (HTTP 403)")
                    
                elif http_works and not websocket_works:
                    # Только HTTP работает (редкий случай)
                    self.http_accessible_channels.add(channel_id)
                    logger.warning(f"   ⚠️ {server}#{channel_name} - HTTP only (not in WebSocket guild data)")
                    
                else:
                    # Ничего не работает
                    failed_completely.append((server, channel_name, channel_id))
                    logger.error(f"   ❌ {server}#{channel_name} - No access via HTTP or WebSocket")
        
        # Выводим итоговую статистику
        logger.info(f"\n📊 Hybrid Verification Results:")
        logger.info(f"   🎉 Full access (HTTP+WS): {len(http_working)} channels")
        logger.info(f"   🔌 WebSocket only: {len(websocket_only)} channels")
        logger.info(f"   📡 Total monitoring: {len(total_monitoring)} channels")
        logger.info(f"   ❌ Failed: {len(failed_completely)} channels")
        
        if total_monitoring:
            logger.success(f"\n🎯 WebSocket will monitor {len(total_monitoring)} channels:")
            for server, channel_name, channel_id, method in total_monitoring:
                logger.info(f"   • {server}#{channel_name} ({method})")
        else:
            logger.error("⚠️ No channels available for monitoring!")
            
        if failed_completely:
            logger.warning(f"\n🚫 These channels have no access:")
            for server, channel_name, channel_id in failed_completely:
                logger.warning(f"   • {server}#{channel_name} - Check permissions")
        
        return len(total_monitoring)
    
    async def process_guild_channels(self, guild_data, ws_session):
        """Process channels from guild data and auto-discover new ones"""
        try:
            guild_name = guild_data['name']
            channels_in_guild = guild_data.get('channels', [])
            
            # Ищем announcement каналы в порядке приоритета:
            # 1. Точное название "announcements"
            # 2. Официальный тип 5
            # 3. Другие варианты названий
            announcement_channels = []
            for channel in channels_in_guild:
                # 1. Точное совпадение с "announcements"
                if channel['type'] == 0 and channel['name'].lower() == 'announcements':
                    announcement_channels.append(channel)
                    continue
                
                # 2. Официальный тип announcement
                if channel.get('type') == 5:
                    announcement_channels.append(channel)
                    continue
                
                # 3. Другие варианты (только если еще не добавлен)
                if (channel['type'] == 0 and 
                    any(keyword in channel['name'].lower() 
                        for keyword in ['announce', 'news', 'объявлен', 'анонс'])):
                    announcement_channels.append(channel)
            
            if announcement_channels:
                logger.info(f"🔍 Found {len(announcement_channels)} announcement channels in {guild_name}")
                
                new_channels_added = 0
                for channel in announcement_channels:
                    channel_id = channel['id']
                    channel_name = channel['name']
                    
                    # Проверяем, есть ли уже в конфиге
                    already_configured = False
                    for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                        if channel_id in channels:
                            already_configured = True
                            break
                    
                    if not already_configured:
                        # Тестируем доступ
                        http_works = await self.test_http_access(
                            channel_id, guild_name, channel_name, ws_session['token']
                        )
                        
                        # WebSocket доступ уже подтвержден (канал в guild data)
                        websocket_works = True
                        
                        if http_works or websocket_works:
                            # Добавляем новый канал
                            if guild_name not in config.SERVER_CHANNEL_MAPPINGS:
                                config.SERVER_CHANNEL_MAPPINGS[guild_name] = {}
                            config.SERVER_CHANNEL_MAPPINGS[guild_name][channel_id] = channel_name
                            
                            # Добавляем в подписки
                            self.subscribed_channels.add(channel_id)
                            if http_works:
                                self.http_accessible_channels.add(channel_id)
                            if websocket_works:
                                self.websocket_accessible_channels.add(channel_id)
                            
                            access_type = "HTTP+WS" if http_works else "WS only"
                            logger.success(f"   ✅ Auto-added: {guild_name}#{channel_name} ({access_type})")
                            
                            # Add channel to subscriptions
                            self.subscribed_channels.add(channel_id)
                            if http_works:
                                self.http_accessible_channels.add(channel_id)
                            if websocket_works:
                                self.websocket_accessible_channels.add(channel_id)
                            
                            # Only create topic once per server
                            if guild_name not in [s for s in config.SERVER_CHANNEL_MAPPINGS.keys()]:
                                if self.telegram_bot:
                                    loop = asyncio.get_event_loop()
                                    loop.run_in_executor(
                                        None,
                                        self.telegram_bot._get_or_create_topic_safe,
                                        guild_name
                                    )
                            
                            new_channels_added += 1
                
                if new_channels_added > 0:
                    logger.info(f"🎉 Auto-discovered {new_channels_added} new channels in {guild_name}")
                        
        except Exception as e:
            logger.error(f"Error processing guild channels: {e}")
    
    async def handle_new_message(self, message_data):
        """Process new message from WebSocket with improved topic management"""
        try:
            channel_id = message_data['channel_id']
            
            # Check if we're subscribed to this channel
            if channel_id not in self.subscribed_channels:
                return  # Ignore unsubscribed channels
            
            # Find channel information
            server_name = None
            channel_name = None
            
            for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                if channel_id in channels:
                    server_name = server
                    channel_name = channels[channel_id]
                    break
            
            if not server_name:
                logger.warning(f"⚠️ Message from subscribed but unmapped channel {channel_id}")
                return
            
            # Safe content processing
            try:
                content = message_data.get('content', '')
                if content:
                    content = content.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                    content = ''.join(char for char in content if ord(char) < 0x110000)
                else:
                    return  # Skip empty messages
            except:
                content = '[Message content encoding error]'
            
            try:
                author = message_data['author']['username']
                author = author.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                author = ''.join(char for char in author if ord(char) < 0x110000)
            except:
                author = 'Unknown User'
            
            # Skip system messages without content
            if not content.strip():
                return
            
            # Create message object
            message = Message(
                content=content,
                timestamp=datetime.fromisoformat(message_data['timestamp'].replace('Z', '+00:00')),
                server_name=server_name,
                channel_name=channel_name,
                author=author
            )
            
            # Determine access type for logging
            access_type = ""
            if channel_id in self.http_accessible_channels and channel_id in self.websocket_accessible_channels:
                access_type = " (HTTP+WS)"
            elif channel_id in self.websocket_accessible_channels:
                access_type = " (WS only)"
            
            logger.info(f"🎉 NEW MESSAGE RECEIVED{access_type}!")
            logger.info(f"   📍 {server_name}#{channel_name}")
            logger.info(f"   👤 {author}")
            logger.info(f"   💬 {content[:100]}...")
            
            # Check if topic exists for this server
            if self.telegram_bot:
                topic_id = self.telegram_bot.get_server_topic_id(server_name)
                if topic_id:
                    logger.info(f"   📍 Will send to existing topic {topic_id}")
                else:
                    logger.info(f"   🔨 Will create new topic for server {server_name}")
            
            # Forward to Telegram
            if self.telegram_bot:
                await self.forward_to_telegram(message)
            else:
                logger.warning("❌ Telegram bot not available")
                
        except Exception as e:
            logger.error(f"❌ Error handling new message: {e}")
    
    async def forward_to_telegram(self, message):
        """Forward message to Telegram bot asynchronously with proper topic management"""
        try:
            logger.info(f"🚀 Forwarding to Telegram: {message.server_name}#{message.channel_name}")
            
            loop = asyncio.get_event_loop()
            
            # First try to get existing topic
            topic_id = await loop.run_in_executor(
                None,
                self.telegram_bot.get_server_topic_id,
                message.server_name
            )
            
            # Only create new topic if none exists and we have permissions
            if topic_id is None:
                if self.telegram_bot._check_if_supergroup_with_topics(config.TELEGRAM_CHAT_ID):
                    logger.info(f"🔍 No existing topic found for {message.server_name}, creating new one...")
                    topic_id = await loop.run_in_executor(
                        None,
                        self.telegram_bot._get_or_create_topic_safe,
                        message.server_name
                    )
                    
                    if topic_id is None:
                        logger.error(f"❌ Failed to get/create topic for {message.server_name}")
                        return
                else:
                    logger.error("❌ Cannot create topic - chat doesn't support topics")
                    return
            else:
                logger.info(f"✅ Using existing topic {topic_id} for {message.server_name}")
            
            # Format and send message
            formatted = self.telegram_bot.format_message(message)
            sent_msg = await loop.run_in_executor(
                None,
                self.telegram_bot._send_message,
                formatted,
                None,  # chat_id
                topic_id,  # message_thread_id
                message.server_name  # server_name for recovery
            )
            
            if sent_msg:
                self.telegram_bot.message_mappings[str(message.timestamp)] = sent_msg.message_id
                self.telegram_bot._save_data()
                
                topic_info = f" to topic {topic_id}" if topic_id else " as regular message"
                logger.success(f"✅ Successfully forwarded{topic_info}")
            else:
                logger.error("❌ Failed to send to Telegram")
            
        except Exception as e:
            logger.error(f"❌ Error forwarding to Telegram: {e}")
    
    async def connect_websocket(self, ws_session):
        """Connect to Discord Gateway WebSocket"""
        try:
            # Получаем URL Gateway
            async with aiohttp.ClientSession() as session:
                async with session.get('https://discord.com/api/v9/gateway') as resp:
                    gateway_data = await resp.json()
                    gateway_url = gateway_data['url']
            
            # Подключаемся к WebSocket
            ws_session['session'] = aiohttp.ClientSession()
            ws_session['websocket'] = await ws_session['session'].ws_connect(
                f"{gateway_url}/?v=9&encoding=json"
            )
            
            logger.info(f"🔗 Connected to Discord Gateway: {gateway_url}")
            
            # Слушаем сообщения
            async for msg in ws_session['websocket']:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self.handle_gateway_message(data, ws_session)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {ws_session['websocket'].exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    logger.warning("WebSocket connection closed")
                    break
                    
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
        finally:
            await self.cleanup_websocket(ws_session)
    
    async def cleanup_websocket(self, ws_session):
        """Clean up WebSocket connection"""
        try:
            if ws_session['heartbeat_task']:
                ws_session['heartbeat_task'].cancel()
                try:
                    await ws_session['heartbeat_task']
                except asyncio.CancelledError:
                    pass
            
            if ws_session['websocket'] and not ws_session['websocket'].closed:
                await ws_session['websocket'].close()
            
            if ws_session['session'] and not ws_session['session'].closed:
                await ws_session['session'].close()
                
        except Exception as e:
            logger.error(f"Error cleaning up WebSocket: {e}")
    
    async def start(self):
        """Start WebSocket connections for all tokens"""
        self.running = True
        logger.info("🚀 Starting Discord WebSocket service with hybrid channel access...")
        
        tasks = []
        for ws_session in self.websockets:
            task = asyncio.create_task(self.connect_websocket(ws_session))
            tasks.append(task)
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error in WebSocket service: {e}")
        finally:
            self.running = False
    
    async def stop(self):
        """Stop WebSocket connections"""
        self.running = False
        logger.info("Stopping Discord WebSocket service...")
        
        for ws_session in self.websockets:
            await self.cleanup_websocket(ws_session)
    
    def add_channel_subscription(self, channel_id):
        """Add a channel to subscription list"""
        self.subscribed_channels.add(channel_id)
        logger.info(f"Added channel {channel_id} to subscriptions")
    
    def remove_channel_subscription(self, channel_id):
        """Remove a channel from subscription list"""
        self.subscribed_channels.discard(channel_id)
        self.http_accessible_channels.discard(channel_id)
        self.websocket_accessible_channels.discard(channel_id)
        logger.info(f"Removed channel {channel_id} from subscriptions")
