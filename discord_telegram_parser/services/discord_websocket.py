import asyncio
import json
import aiohttp
import time
from datetime import datetime, timedelta
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
        self.http_accessible_channels = set()
        self.websocket_accessible_channels = set()
        self.running = False
        
        # Новые атрибуты для управления верификацией
        self.pending_servers = {}  # server_id -> {'join_time': datetime, 'verified': bool}
        self.verification_delay = 180  # 3 минуты задержки
        self.server_verification_cache = {}  # Кэш верифицированных серверов
        
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
                    
                    # Проверяем существующие топики при запуске
                    await self.verify_existing_topics()
                    
                    # Используем гибридный подход для верификации каналов
                    await self.hybrid_channel_verification(ws_session, guilds)
                    
                elif event_type == 'GUILD_CREATE':
                    # Новый сервер добавлен или стал доступен
                    guild = data['d']
                    await self.handle_new_guild(guild, ws_session)
                    
                elif event_type == 'MESSAGE_CREATE':
                    await self.handle_new_message(data['d'])
                    
        except Exception as e:
            logger.error(f"Error handling gateway message: {e}")
    
    async def verify_existing_topics(self):
        """Проверяем существующие топики при запуске и удаляем дубли"""
        if not self.telegram_bot:
            return
            
        logger.info("🔍 Verifying existing topics to prevent duplicates...")
        
        # Получаем список всех топиков в чате
        try:
            existing_topics = await self.get_all_forum_topics()
            topic_names = {}  # name -> topic_id
            
            # Группируем топики по именам для поиска дублей
            for topic_id, topic_name in existing_topics.items():
                clean_name = topic_name.replace("🏰 ", "").strip()
                if clean_name in topic_names:
                    # Найден дубль! Удаляем старый топик
                    old_topic_id = topic_names[clean_name]
                    logger.warning(f"🗑️ Found duplicate topic for '{clean_name}': keeping {topic_id}, removing {old_topic_id}")
                    await self.close_forum_topic(old_topic_id)
                    
                    # Обновляем mapping на новый топик
                    if clean_name in self.telegram_bot.server_topics:
                        if self.telegram_bot.server_topics[clean_name] == old_topic_id:
                            self.telegram_bot.server_topics[clean_name] = topic_id
                            logger.info(f"🔄 Updated topic mapping for '{clean_name}': {old_topic_id} -> {topic_id}")
                
                topic_names[clean_name] = topic_id
            
            # Синхронизируем с нашим кэшем
            for server_name, cached_topic_id in list(self.telegram_bot.server_topics.items()):
                if server_name in topic_names:
                    actual_topic_id = topic_names[server_name]
                    if cached_topic_id != actual_topic_id:
                        logger.info(f"🔄 Syncing topic for '{server_name}': {cached_topic_id} -> {actual_topic_id}")
                        self.telegram_bot.server_topics[server_name] = actual_topic_id
                else:
                    # Топик не существует в Telegram, удаляем из кэша
                    logger.warning(f"🗑️ Removing non-existent topic from cache: '{server_name}' -> {cached_topic_id}")
                    del self.telegram_bot.server_topics[server_name]
            
            self.telegram_bot._save_data()
            logger.success(f"✅ Topic verification complete. Active topics: {len(self.telegram_bot.server_topics)}")
            
        except Exception as e:
            logger.error(f"❌ Error verifying existing topics: {e}")
    
    async def get_all_forum_topics(self):
        """Получить все форум-топики в чате"""
        topics = {}
        try:
            # Используем Telegram Bot API для получения топиков
            loop = asyncio.get_event_loop()
            chat_id = config.TELEGRAM_CHAT_ID
            
            # Проверяем, поддерживает ли чат топики
            def check_chat():
                try:
                    chat = self.telegram_bot.bot.get_chat(chat_id)
                    return getattr(chat, 'is_forum', False)
                except:
                    return False
            
            is_forum = await loop.run_in_executor(None, check_chat)
            if not is_forum:
                return topics
            
            # К сожалению, Telegram Bot API не предоставляет метод для получения всех топиков
            # Поэтому мы проверяем только те, что у нас в кэше
            for server_name, topic_id in list(self.telegram_bot.server_topics.items()):
                def check_topic():
                    return self.telegram_bot._topic_exists(chat_id, topic_id)
                
                exists = await loop.run_in_executor(None, check_topic)
                if exists:
                    topics[topic_id] = server_name
                    
        except Exception as e:
            logger.error(f"Error getting forum topics: {e}")
            
        return topics
    
    async def close_forum_topic(self, topic_id):
        """Закрыть форум-топик"""
        try:
            loop = asyncio.get_event_loop()
            chat_id = config.TELEGRAM_CHAT_ID
            
            def close_topic():
                try:
                    self.telegram_bot.bot.close_forum_topic(
                        chat_id=chat_id,
                        message_thread_id=topic_id
                    )
                    return True
                except Exception as e:
                    logger.error(f"Error closing topic {topic_id}: {e}")
                    return False
            
            result = await loop.run_in_executor(None, close_topic)
            if result:
                logger.info(f"🔒 Closed duplicate topic {topic_id}")
            
        except Exception as e:
            logger.error(f"Error closing forum topic {topic_id}: {e}")
    
    async def handle_new_guild(self, guild_data, ws_session):
        """Обработка нового сервера с учетом верификации"""
        guild_id = guild_data['id']
        guild_name = guild_data['name']
        
        logger.info(f"🆕 New guild detected: {guild_name} (ID: {guild_id})")
        
        # Проверяем, известен ли нам этот сервер
        is_new_server = guild_name not in config.SERVER_CHANNEL_MAPPINGS
        
        if is_new_server:
            logger.info(f"🔍 Completely new server: {guild_name}")
            
            # Добавляем в pending с временной меткой
            self.pending_servers[guild_id] = {
                'name': guild_name,
                'join_time': datetime.now(),
                'verified': False,
                'channels_discovered': False
            }
            
            # Планируем отложенную обработку
            asyncio.create_task(self.delayed_server_processing(guild_id, guild_data, ws_session))
            
        else:
            # Известный сервер, обрабатываем сразу
            logger.info(f"♻️ Known server reconnected: {guild_name}")
            await self.process_guild_channels(guild_data, ws_session)
    
    async def delayed_server_processing(self, guild_id, guild_data, ws_session):
        """Отложенная обработка нового сервера после верификации"""
        guild_name = guild_data['name']
        
        logger.info(f"⏰ Scheduling delayed processing for {guild_name} (waiting {self.verification_delay}s for verification)")
        
        # Ждем указанное время или пока не пройдем верификацию
        start_time = datetime.now()
        while (datetime.now() - start_time).seconds < self.verification_delay:
            # Проверяем, прошли ли мы верификацию досрочно
            if await self.check_server_verification(guild_id, ws_session):
                logger.success(f"✅ Early verification passed for {guild_name}")
                break
                
            await asyncio.sleep(10)  # Проверяем каждые 10 секунд
        
        # Обновляем статус верификации
        if guild_id in self.pending_servers:
            self.pending_servers[guild_id]['verified'] = True
        
        logger.info(f"🚀 Starting delayed processing for {guild_name}")
        
        # Обрабатываем каналы сервера
        await self.process_guild_channels(guild_data, ws_session)
        
        # Создаем топик и отправляем последние сообщения
        await self.setup_new_server_topic(guild_data, ws_session)
        
        # Удаляем из pending
        if guild_id in self.pending_servers:
            del self.pending_servers[guild_id]
    
    async def check_server_verification(self, guild_id, ws_session):
        """Проверка прохождения верификации на сервере"""
        try:
            # Пытаемся получить доступ к каналам сервера
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': ws_session['token']}
                
                async with session.get(
                    f'https://discord.com/api/v9/guilds/{guild_id}/channels',
                    headers=headers
                ) as resp:
                    if resp.status == 200:
                        channels = await resp.json()
                        # Проверяем, есть ли доступные каналы
                        accessible_channels = [ch for ch in channels if ch.get('type') in [0, 5]]
                        return len(accessible_channels) > 0
                    
        except Exception as e:
            logger.debug(f"Verification check failed for guild {guild_id}: {e}")
            
        return False
    
    async def setup_new_server_topic(self, guild_data, ws_session):
        """Создание топика и отправка последних сообщений для нового сервера"""
        guild_name = guild_data['name']
        
        if not self.telegram_bot:
            logger.warning("❌ Telegram bot not available for topic creation")
            return
        
        logger.info(f"🏗️ Setting up topic for new server: {guild_name}")
        
        # Создаем топик (используя безопасный метод без дублей)
        loop = asyncio.get_event_loop()
        topic_id = await loop.run_in_executor(
            None,
            self.telegram_bot._get_or_create_topic_safe,
            guild_name
        )
        
        if not topic_id:
            logger.error(f"❌ Failed to create topic for {guild_name}")
            return
        
        logger.success(f"✅ Created topic {topic_id} for {guild_name}")
        
        # Получаем последние сообщения из announcement каналов
        announcement_channels = []
        if guild_name in config.SERVER_CHANNEL_MAPPINGS:
            for channel_id, channel_name in config.SERVER_CHANNEL_MAPPINGS[guild_name].items():
                announcement_channels.append((channel_id, channel_name))
        
        # Собираем сообщения из всех announcement каналов
        all_messages = []
        for channel_id, channel_name in announcement_channels[:3]:  # Максимум 3 канала
            try:
                # Пытаемся получить последние сообщения
                if self.telegram_bot.discord_parser:
                    messages = self.telegram_bot.discord_parser.parse_announcement_channel(
                        channel_id,
                        guild_name,
                        channel_name,
                        limit=5  # По 5 сообщений с каждого канала
                    )
                    all_messages.extend(messages)
                    logger.info(f"📥 Collected {len(messages)} messages from #{channel_name}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not collect messages from {guild_name}#{channel_name}: {e}")
        
        # Отправляем сообщения в хронологическом порядке
        if all_messages:
            all_messages.sort(key=lambda x: x.timestamp)
            all_messages = all_messages[-10:]  # Последние 10 сообщений
            
            logger.info(f"📤 Sending {len(all_messages)} welcome messages to {guild_name}")
            
            # Отправляем приветственное сообщение
            welcome_msg = f"🎉 Welcome to {guild_name}!\n\n📋 Latest announcements from this server:"
            await loop.run_in_executor(
                None,
                self.telegram_bot._send_message,
                welcome_msg,
                None,  # chat_id
                topic_id,  # message_thread_id
                guild_name  # server_name
            )
            
            # Отправляем сообщения
            self.telegram_bot.send_messages(all_messages)
            
        else:
            # Отправляем просто приветственное сообщение
            welcome_msg = f"🎉 Welcome to {guild_name}!\n\n📡 Now monitoring announcements from this server."
            await loop.run_in_executor(
                None,
                self.telegram_bot._send_message,
                welcome_msg,
                None,  # chat_id
                topic_id,  # message_thread_id
                guild_name  # server_name
            )
    
    async def process_guild_channels(self, guild_data, ws_session):
        """Process channels from guild data and auto-discover new ones"""
        try:
            guild_name = guild_data['name']
            guild_id = guild_data['id']
            channels_in_guild = guild_data.get('channels', [])
            
            logger.info(f"🔍 Processing channels for guild: {guild_name}")
            
            # Find first channel with name ending in "announcement" or "announcements"
            announcement_channels = []
            for channel in channels_in_guild:
                lower_name = channel['name'].lower()
                if (lower_name.endswith('announcement') or 
                    lower_name.endswith('announcements')):
                    announcement_channels.append(channel)
                    break  # Only keep first match
            
            if not announcement_channels:
                logger.info(f"ℹ️ No announcement channels found in {guild_name}")
                return
            
            logger.info(f"🔍 Found {len(announcement_channels)} announcement channels in {guild_name}")
            
            new_channels_added = 0
            
            # Проверяем, есть ли уже конфигурация для этого сервера
            if guild_name not in config.SERVER_CHANNEL_MAPPINGS:
                config.SERVER_CHANNEL_MAPPINGS[guild_name] = {}
            
            for channel in announcement_channels:
                channel_id = channel['id']
                channel_name = channel['name']
                
                # Проверяем, есть ли уже в конфиге
                if channel_id not in config.SERVER_CHANNEL_MAPPINGS[guild_name]:
                    # Тестируем доступ
                    http_works = await self.test_http_access(
                        channel_id, guild_name, channel_name, ws_session['token']
                    )
                    
                    # WebSocket доступ уже подтвержден (канал в guild data)
                    websocket_works = True
                    
                    if http_works or websocket_works:
                        # Добавляем новый канал
                        config.SERVER_CHANNEL_MAPPINGS[guild_name][channel_id] = channel_name
                        
                        # Добавляем в подписки
                        self.subscribed_channels.add(channel_id)
                        if http_works:
                            self.http_accessible_channels.add(channel_id)
                        if websocket_works:
                            self.websocket_accessible_channels.add(channel_id)
                        
                        access_type = "HTTP+WS" if http_works else "WS only"
                        logger.success(f"   ✅ Auto-added: {guild_name}#{channel_name} ({access_type})")
                        new_channels_added += 1
            
            if new_channels_added > 0:
                logger.info(f"🎉 Auto-discovered {new_channels_added} new channels in {guild_name}")
                
                # Обновляем кэш верификации
                self.server_verification_cache[guild_id] = {
                    'verified': True,
                    'timestamp': datetime.now()
                }
            
        except Exception as e:
            logger.error(f"Error processing guild channels: {e}")
    
    # Остальные методы остаются без изменений...
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
                    return resp.status == 200
                        
        except Exception as e:
            return False
    
    def add_channel_subscription(self, channel_id):
        """Add a channel to subscription list"""
        self.subscribed_channels.add(channel_id)
        logger.info(f"Added channel {channel_id} to subscriptions")
    
    # Остальные методы класса остаются без изменений...
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
    
    def check_websocket_channel_access(self, channel_id, guilds_data):
        """Check if channel is accessible via WebSocket guild data"""
        for guild in guilds_data:
            channels = guild.get('channels', [])
            for channel in channels:
                if channel['id'] == channel_id:
                    lower_name = channel['name'].lower()
                    if (lower_name.endswith('announcement') or 
                        lower_name.endswith('announcements')):
                        return True
        return False
    
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
    
    def remove_channel_subscription(self, channel_id):
        """Remove a channel from subscription list"""
        self.subscribed_channels.discard(channel_id)
        self.http_accessible_channels.discard(channel_id)
        self.websocket_accessible_channels.discard(channel_id)
        logger.info(f"Removed channel {channel_id} from subscriptions")
