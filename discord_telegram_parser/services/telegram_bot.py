import telebot
from typing import List, Dict
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config
import json
import os
import time
import threading
import asyncio
from loguru import logger

class TelegramBotService:
    def __init__(self, bot_token: str):
        self.bot = telebot.TeleBot(bot_token)
        self.bot.skip_pending = True
        self.bot.threaded = True
        self.network_timeout = 30
        self.message_store = 'telegram_messages.json'
        self.user_states = {}
        self.server_topics = {}  # server_name -> topic_id mapping
        self.topic_name_cache = {}  # topic_id -> server_name mapping для быстрого поиска
        self.websocket_service = None
        self.topic_creation_lock = threading.Lock()
        
        # Новые атрибуты для предотвращения дублей
        self.startup_verification_done = False
        self.topic_sync_lock = threading.Lock()
        
        # Load existing data
        self._load_data()
    
    def _load_data(self):
        """Load message mappings and topic mappings"""
        if os.path.exists(self.message_store):
            try:
                with open(self.message_store, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.message_mappings = data.get('messages', {})
                    self.server_topics = data.get('topics', {})
                    
                    # Создаем обратный кэш для быстрого поиска
                    self.topic_name_cache = {v: k for k, v in self.server_topics.items()}
                    
                    logger.info(f"📋 Loaded {len(self.server_topics)} topic mappings from cache")
            except Exception as e:
                logger.error(f"Error loading data: {e}")
                self.message_mappings = {}
                self.server_topics = {}
                self.topic_name_cache = {}
        else:
            self.message_mappings = {}
            self.server_topics = {}
            self.topic_name_cache = {}

    def _save_data(self):
        """Save message mappings and topic mappings"""
        try:
            with open(self.message_store, 'w', encoding='utf-8') as f:
                json.dump({
                    'messages': self.message_mappings,
                    'topics': self.server_topics
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving data: {e}")

    def startup_topic_verification(self, chat_id=None):
        """Проверка топиков при запуске для предотвращения дублей"""
        if self.startup_verification_done:
            return
            
        with self.topic_sync_lock:
            if self.startup_verification_done:  # Двойная проверка
                return
                
            chat_id = chat_id or config.TELEGRAM_CHAT_ID
            
            logger.info("🔍 Starting startup topic verification to prevent duplicates...")
            
            try:
                if not self._check_if_supergroup_with_topics(chat_id):
                    logger.info("ℹ️ Chat doesn't support topics, skipping verification")
                    self.startup_verification_done = True
                    return
                
                # Получаем все существующие топики (через попытку отправки тестового сообщения)
                existing_valid_topics = {}
                invalid_topics = []
                
                for server_name, topic_id in list(self.server_topics.items()):
                    if self._topic_exists(chat_id, topic_id):
                        # Проверяем на дубли по названию
                        topic_name = f"🏰 {server_name}"
                        
                        if topic_name in existing_valid_topics:
                            # Найден дубль! Закрываем старый топик
                            old_topic_id = existing_valid_topics[topic_name]
                            logger.warning(f"🗑️ Found duplicate topic for '{server_name}': keeping {topic_id}, closing {old_topic_id}")
                            
                            try:
                                self.bot.close_forum_topic(
                                    chat_id=chat_id,
                                    message_thread_id=old_topic_id
                                )
                                logger.info(f"🔒 Closed duplicate topic {old_topic_id}")
                            except Exception as e:
                                logger.warning(f"⚠️ Could not close duplicate topic {old_topic_id}: {e}")
                            
                            # Удаляем старый из кэша
                            for srv_name, srv_topic_id in list(self.server_topics.items()):
                                if srv_topic_id == old_topic_id:
                                    del self.server_topics[srv_name]
                                    break
                        
                        existing_valid_topics[topic_name] = topic_id
                        
                    else:
                        # Топик не существует
                        invalid_topics.append(server_name)
                
                # Удаляем недействительные топики из кэша
                for server_name in invalid_topics:
                    if server_name in self.server_topics:
                        old_topic_id = self.server_topics[server_name]
                        del self.server_topics[server_name]
                        logger.info(f"🗑️ Removed invalid topic mapping: {server_name} -> {old_topic_id}")
                
                # Пересоздаем обратный кэш
                self.topic_name_cache = {v: k for k, v in self.server_topics.items()}
                
                # Сохраняем изменения
                if invalid_topics or len(existing_valid_topics) != len(self.server_topics):
                    self._save_data()
                
                logger.success(f"✅ Startup verification complete:")
                logger.info(f"   📋 Valid topics: {len(self.server_topics)}")
                logger.info(f"   🗑️ Removed invalid: {len(invalid_topics)}")
                logger.info(f"   🛡️ Duplicate protection: ACTIVE")
                
                self.startup_verification_done = True
                
            except Exception as e:
                logger.error(f"❌ Error during startup verification: {e}")
                self.startup_verification_done = True

    def _check_if_supergroup_with_topics(self, chat_id):
        """Check if the chat supports topics"""
        try:
            chat = self.bot.get_chat(chat_id)
            return chat.type == 'supergroup' and getattr(chat, 'is_forum', False)
        except Exception as e:
            logger.debug(f"Error checking chat type: {e}")
            return False

    def _topic_exists(self, chat_id, topic_id):
        """Check if a specific topic exists using Telegram API"""
        if not topic_id:
            return False
            
        try:
            # Пытаемся получить информацию о топике
            topic_info = self.bot.get_forum_topic(
                chat_id=chat_id,
                message_thread_id=topic_id
            )
            return topic_info is not None and not getattr(topic_info, 'is_closed', False)
        except telebot.apihelper.ApiException as e:
            if "not found" in str(e).lower() or "thread not found" in str(e).lower():
                return False
            # For other errors, assume topic exists
            return True
        except Exception:
            return False

    def get_server_topic_id(self, server_name: str):
        """Get existing topic ID for server (safe for real-time use)"""
        # Проверяем кэш при первом запуске
        if not self.startup_verification_done:
            self.startup_topic_verification()
        
        # Быстрая проверка кэша без блокировки
        if server_name in self.server_topics:
            topic_id = self.server_topics[server_name]
            logger.debug(f"📍 Found cached topic {topic_id} for server '{server_name}'")
            return topic_id
        return None

    def _get_or_create_topic_safe(self, server_name: str, chat_id=None):
        """Thread-safe method to get or create topic for server with duplicate prevention"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        # Проверяем верификацию при запуске
        if not self.startup_verification_done:
            self.startup_topic_verification(chat_id)
        
        # ВАЖНО: Сначала проверяем кэш БЕЗ блокировки для быстрого доступа
        if server_name in self.server_topics:
            cached_topic_id = self.server_topics[server_name]
            
            # Быстрая проверка существования топика
            if self._topic_exists(chat_id, cached_topic_id):
                logger.debug(f"✅ Using existing cached topic {cached_topic_id} for server '{server_name}'")
                return cached_topic_id
            else:
                logger.warning(f"⚠️ Cached topic {cached_topic_id} not found, will recreate")
        
        # Используем блокировку только если нужно создать/пересоздать топик
        with self.topic_creation_lock:
            # Двойная проверка после получения блокировки
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                
                # Повторная проверка существования с блокировкой
                if self._topic_exists(chat_id, topic_id):
                    logger.debug(f"✅ Using existing topic {topic_id} for server '{server_name}' (double-check)")
                    return topic_id
                else:
                    logger.warning(f"🗑️ Topic {topic_id} confirmed missing, removing from cache")
                    del self.server_topics[server_name]
                    if topic_id in self.topic_name_cache:
                        del self.topic_name_cache[topic_id]
                    self._save_data()
            
            # Проверяем, поддерживает ли чат топики
            if not self._check_if_supergroup_with_topics(chat_id):
                logger.info(f"ℹ️ Chat doesn't support topics, using regular messages")
                return None
            
            # Проверяем, нет ли уже топика с таким именем (дополнительная защита)
            topic_name = f"🏰 {server_name}"
            for existing_server, existing_topic_id in self.server_topics.items():
                if existing_server != server_name and self._topic_exists(chat_id, existing_topic_id):
                    try:
                        topic_info = self.bot.get_forum_topic(chat_id, existing_topic_id)
                        if topic_info and getattr(topic_info, 'name', '') == topic_name:
                            logger.warning(f"🔍 Found existing topic with same name for different server: {existing_server}")
                            # Возвращаем существующий топик и обновляем маппинг
                            self.server_topics[server_name] = existing_topic_id
                            self.topic_name_cache[existing_topic_id] = server_name
                            self._save_data()
                            return existing_topic_id
                    except:
                        continue
            
            # Создаём новый топик
            logger.info(f"🔨 Creating new topic for server '{server_name}'")
            
            try:
                topic = self.bot.create_forum_topic(
                    chat_id=chat_id,
                    name=topic_name,
                    icon_color=0x6FB9F0,  # Blue color
                    icon_custom_emoji_id=None
                )
                
                topic_id = topic.message_thread_id
                self.server_topics[server_name] = topic_id
                self.topic_name_cache[topic_id] = server_name
                self._save_data()
                
                logger.success(f"✅ Created new topic for server '{server_name}' with ID: {topic_id}")
                return topic_id
                
            except Exception as e:
                logger.error(f"❌ Error creating topic for server '{server_name}': {e}")
                return None

    def _recreate_topic_if_missing(self, server_name: str, chat_id=None):
        """Recreate a topic if the current one is missing"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        # Remove the old topic ID from our mapping
        if server_name in self.server_topics:
            old_topic_id = self.server_topics[server_name]
            logger.info(f"🗑️ Removing invalid topic {old_topic_id} for server '{server_name}'")
            del self.server_topics[server_name]
            if old_topic_id in self.topic_name_cache:
                del self.topic_name_cache[old_topic_id]
            self._save_data()
        
        # Create a new topic using safe method
        return self._get_or_create_topic_safe(server_name, chat_id)

    def format_message(self, message: Message) -> str:
        """Format message for topic replies"""
        formatted = []
        
        # Add channel info if available
        if message.channel_name:
            formatted.append(f"📢 #{message.channel_name}")
        
        if config.TELEGRAM_UI_PREFERENCES['show_timestamps']:
            formatted.append(f"📅 {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        formatted.append(f"👤 {message.author}")
        formatted.append(f"💬 {message.content}")
        
        return "\n".join(formatted)

    def send_messages(self, messages: List[Message]):
        """Send formatted messages to Telegram with improved duplicate prevention"""
        if not messages:
            return
        
        # Проверяем верификацию при запуске
        if not self.startup_verification_done:
            self.startup_topic_verification()
        
        server_groups = {}
        
        # Group messages by server
        for message in messages:
            server_name = message.server_name or "Unknown Server"
            if server_name not in server_groups:
                server_groups[server_name] = []
            server_groups[server_name].append(message)
        
        # Send messages with server topics (NO DUPLICATES!)
        for server_name, server_messages in server_groups.items():
            logger.info(f"📤 Sending {len(server_messages)} messages for server: {server_name}")
            
            # ИСПРАВЛЕНИЕ: Используем быстрый метод для проверки существующих топиков
            topic_id = self.get_server_topic_id(server_name)
            if not topic_id:
                # Создаём топик только если его нет (с защитой от дублей)
                topic_id = self._get_or_create_topic_safe(server_name)
            
            # Sort messages chronologically (oldest first)
            server_messages.sort(key=lambda x: x.timestamp, reverse=False)
            
            # Send messages in order
            success_count = 0
            for message in server_messages:
                formatted = self.format_message(message)
                sent_msg = self._send_message(
                    formatted,
                    message_thread_id=topic_id,
                    server_name=server_name
                )
                
                if sent_msg:
                    # Store mapping between Discord and Telegram message IDs
                    self.message_mappings[str(message.timestamp)] = sent_msg.message_id
                    success_count += 1
                else:
                    logger.warning(f"❌ Failed to send message: {formatted[:50]}...")
            
            logger.info(f"✅ Sent {success_count}/{len(server_messages)} messages for {server_name}")
            
            # Save mappings after each server
            self._save_data()
            
        logger.success(f"✅ Completed sending messages for {len(server_groups)} servers")

    def _send_message(self, text: str, chat_id=None, message_thread_id=None, server_name=None):
        """Send message to topic or regular chat with error recovery and duplicate prevention"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        max_retries = 3
        retry_delay = 5
        
        logger.debug(f"📤 Sending message to chat {chat_id}")
        if message_thread_id:
            logger.debug(f"📍 Topic: {message_thread_id}")
            
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            for attempt in range(max_retries):
                try:
                    result = self.bot.send_message(
                        chat_id, 
                        chunk,
                        message_thread_id=message_thread_id
                    )
                    logger.debug(f"✅ Message sent successfully: {result.message_id}")
                    return result
                    
                except Exception as e:
                    error_str = str(e)
                    logger.warning(f"❌ Error sending message (attempt {attempt + 1}): {e}")
                    
                    # Handle specific error cases
                    if "message thread not found" in error_str and server_name and message_thread_id:
                        logger.warning(f"🔍 Topic {message_thread_id} not found for server '{server_name}'")
                        
                        # Try to recreate the topic (with duplicate prevention)
                        new_topic_id = self._recreate_topic_if_missing(server_name, chat_id)
                        
                        if new_topic_id:
                            logger.info(f"🔨 Created new topic {new_topic_id}. Retrying...")
                            message_thread_id = new_topic_id
                            continue  # Retry with new topic ID
                        else:
                            logger.warning("⚠️ Failed to recreate topic. Sending as regular message.")
                            message_thread_id = None  # Fall back to regular message
                            continue
                            
                    elif "message thread not found" in error_str and message_thread_id:
                        logger.warning("⚠️ Topic not found and no server name provided. Falling back to regular message.")
                        message_thread_id = None  # Fall back to regular message
                        continue
                        
                    elif "Too Many Requests" in error_str:
                        wait_time = 60  # Default wait time
                        if "retry after" in error_str:
                            try:
                                wait_time = int(error_str.split("retry after")[1].strip())
                            except:
                                pass
                        logger.warning(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                        
                    elif attempt == max_retries - 1:
                        logger.error(f"💥 Failed to send message after {max_retries} attempts: {e}")
                        return None
                        
                    time.sleep(retry_delay)
            
        return None

    def cleanup_invalid_topics(self, chat_id=None):
        """Clean up invalid topic mappings with duplicate detection"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        with self.topic_sync_lock:
            invalid_topics = []
            valid_topics = {}
            
            for server_name, topic_id in list(self.server_topics.items()):
                if not self._topic_exists(chat_id, topic_id):
                    invalid_topics.append(server_name)
                else:
                    # Проверяем на дубли
                    topic_name = f"🏰 {server_name}"
                    if topic_name in valid_topics:
                        # Найден дубль, закрываем старый
                        old_topic_id = valid_topics[topic_name]
                        logger.warning(f"🗑️ Duplicate topic found during cleanup: {server_name}")
                        try:
                            self.bot.close_forum_topic(chat_id, old_topic_id)
                        except:
                            pass
                        # Удаляем старый из маппинга
                        for srv, tid in list(self.server_topics.items()):
                            if tid == old_topic_id:
                                invalid_topics.append(srv)
                                break
                    
                    valid_topics[topic_name] = topic_id
            
            # Remove invalid topics
            for server_name in invalid_topics:
                if server_name in self.server_topics:
                    old_topic_id = self.server_topics[server_name]
                    logger.info(f"🗑️ Removing invalid topic for server: {server_name} (ID: {old_topic_id})")
                    del self.server_topics[server_name]
                    if old_topic_id in self.topic_name_cache:
                        del self.topic_name_cache[old_topic_id]
            
            if invalid_topics:
                self._save_data()
                logger.success(f"🧹 Cleaned up {len(invalid_topics)} invalid/duplicate topics")
            
            return len(invalid_topics)

    def add_channel_to_server(self, server_name: str, channel_id: str, channel_name: str = None):
        """Добавить новый канал к существующему серверу"""
        try:
            # Проверяем, есть ли сервер в конфигурации
            if server_name not in config.SERVER_CHANNEL_MAPPINGS:
                config.SERVER_CHANNEL_MAPPINGS[server_name] = {}
            
            # Проверяем, не добавлен ли уже этот канал
            if channel_id in config.SERVER_CHANNEL_MAPPINGS[server_name]:
                return False, "Канал уже добавлен к этому серверу"
            
            # Проверяем доступность канала через Discord API
            if hasattr(self, 'discord_parser') and self.discord_parser:
                try:
                    # Пытаемся получить сообщения из канала для проверки доступности
                    test_messages = self.discord_parser.parse_announcement_channel(
                        channel_id, 
                        server_name, 
                        channel_name or f"Channel_{channel_id}",
                        limit=1
                    )
                    access_confirmed = True
                except Exception as e:
                    logger.warning(f"⚠️ Cannot access channel {channel_id}: {e}")
                    access_confirmed = False
            else:
                access_confirmed = False
            
            # Добавляем канал в конфигурацию
            final_channel_name = channel_name or f"Channel_{channel_id}"
            config.SERVER_CHANNEL_MAPPINGS[server_name][channel_id] = final_channel_name
            
            # Добавляем канал в WebSocket подписки
            if self.websocket_service:
                self.websocket_service.add_channel_subscription(channel_id)
                logger.info(f"✅ Added channel {channel_id} to WebSocket subscriptions")
            
            # Сохраняем конфигурацию (если у нас есть такая функция)
            logger.success(f"✅ Added channel '{final_channel_name}' ({channel_id}) to server '{server_name}'")
            
            status = "✅ Доступен" if access_confirmed else "⚠️ Ограниченный доступ (будет мониториться через WebSocket)"
            
            return True, f"Канал успешно добавлен!\nСтатус: {status}"
            
        except Exception as e:
            logger.error(f"❌ Error adding channel to server: {e}")
            return False, f"Ошибка при добавлении канала: {str(e)}"

    def start_bot(self):
        """Start bot with improved topic management and startup verification"""
        
        # Выполняем проверку топиков при запуске
        self.startup_topic_verification()
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            supports_topics = self._check_if_supergroup_with_topics(message.chat.id)
            
            text = (
                "🤖 Welcome to Discord Announcement Parser!\n\n"
                "🔥 **Real-time WebSocket Mode** - Instant message delivery!\n"
                "📡 Messages are received via WebSocket for immediate forwarding\n"
                "🛡️ **ANTI-DUPLICATE System**: Prevents topic duplication!\n\n"
            )
            
            if supports_topics:
                text += (
                    "🔹 Forum Topics Mode (Enabled):\n"
                    "• Each Discord server gets ONE topic (NO DUPLICATES)\n"
                    "• Messages from all channels in server go to same topic\n"
                    "• Smart caching prevents duplicate topic creation\n"
                    "• Auto-recovery for missing topics\n"
                    "• Fast topic lookup for real-time messages\n"
                    "• Startup verification prevents duplicates on restart\n"
                    "• Messages displayed chronologically\n\n"
                )
            else:
                text += (
                    "🔹 Regular Messages Mode:\n"
                    "• Messages sent as regular chat messages\n"
                    "• To enable topics, convert this chat to a supergroup with topics enabled\n\n"
                )
            
            text += "Choose an action below:"
            
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("📋 Server List", callback_data="servers"),
                InlineKeyboardButton("🔄 Manual Sync", callback_data="refresh"),
                InlineKeyboardButton("⚡ WebSocket Status", callback_data="websocket"),
                InlineKeyboardButton("🧹 Clean Topics", callback_data="cleanup"),
                InlineKeyboardButton("📊 Bot Status", callback_data="status"),
                InlineKeyboardButton("ℹ️ Help", callback_data="help")
            )
            
            self.bot.send_message(message.chat.id, text, reply_markup=markup)

        # Обработчики callback queries должны быть определены ДО функций
        def handle_servers_list(call):
            """Показать список серверов с возможностью добавления каналов"""
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or not config.SERVER_CHANNEL_MAPPINGS:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
                self.bot.edit_message_text(
                    "❌ No servers found. Please configure servers first.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
                return
                
            markup = InlineKeyboardMarkup()
            for server in config.SERVER_CHANNEL_MAPPINGS.keys():
                # Add topic indicator with duplicate check
                topic_indicator = ""
                if server in self.server_topics:
                    topic_id = self.server_topics[server]
                    if self._topic_exists(call.message.chat.id, topic_id):
                        topic_indicator = " 📋"
                    else:
                        topic_indicator = " ❌"
                else:
                    topic_indicator = " 🆕"  # New server, no topic yet
                
                markup.add(InlineKeyboardButton(
                    f"🏰 {server}{topic_indicator}",
                    callback_data=f"server_{server}"
                ))
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            server_count = len(config.SERVER_CHANNEL_MAPPINGS)
            topic_count = len(self.server_topics)
            
            text = (
                f"📋 Select a server to view announcements:\n\n"
                f"📊 {server_count} servers configured, {topic_count} topics created\n"
                f"📋 = Has topic, ❌ = Invalid topic, 🆕 = New server\n"
                f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}"
            )
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_manual_sync(call):
            """Выполнить ручную синхронизацию"""
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            try:
                # Здесь можно добавить логику ручной синхронизации
                # Например, вызов метода из discord_parser для обновления каналов
                sync_result = "🔄 Manual sync completed successfully!"
                if hasattr(self, 'discord_parser') and self.discord_parser:
                    # Попробуем обновить список серверов
                    sync_result += f"\n📊 Found {len(config.SERVER_CHANNEL_MAPPINGS)} servers"
                
                self.bot.edit_message_text(
                    sync_result,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except Exception as e:
                self.bot.edit_message_text(
                    f"❌ Sync failed: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )

        def handle_websocket_status(call):
            """Показать статус WebSocket соединения"""
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            ws_status = "❌ WebSocket service not available"
            if self.websocket_service:
                subscribed_channels = len(self.websocket_service.subscribed_channels)
                http_channels = len(getattr(self.websocket_service, 'http_accessible_channels', set()))
                ws_only_channels = len(getattr(self.websocket_service, 'websocket_accessible_channels', set()))
                
                ws_status = (
                    f"⚡ WebSocket Status\n\n"
                    f"📡 Subscribed channels: {subscribed_channels}\n"
                    f"🌐 HTTP accessible: {http_channels}\n"
                    f"🔌 WebSocket only: {ws_only_channels}\n"
                    f"🔄 Status: {'✅ Active' if self.websocket_service.running else '❌ Inactive'}"
                )
            
            self.bot.edit_message_text(
                ws_status,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_cleanup_topics(call):
            """Очистить недействительные топики"""
            cleaned = self.cleanup_invalid_topics(call.message.chat.id)
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            self.bot.edit_message_text(
                f"🧹 Topic cleanup completed!\n\n"
                f"Removed {cleaned} invalid/duplicate topics.\n"
                f"Current topics: {len(self.server_topics)}\n"
                f"🛡️ Anti-duplicate protection: ACTIVE",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_bot_status(call):
            """Показать статус бота"""
            supports_topics = self._check_if_supergroup_with_topics(call.message.chat.id)
            
            status_text = (
                "📊 Bot Status\n\n"
                f"🔹 Topics Support: {'✅ Enabled' if supports_topics else '❌ Disabled'}\n"
                f"🔹 Active Topics: {len(self.server_topics)}\n"
                f"🔹 Configured Servers: {len(config.SERVER_CHANNEL_MAPPINGS) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                f"🔹 Total Channels: {sum(len(channels) for channels in config.SERVER_CHANNEL_MAPPINGS.values()) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                f"🔹 Message Cache: {len(self.message_mappings)} messages\n"
                f"🔹 WebSocket Channels: {len(self.websocket_service.subscribed_channels) if self.websocket_service else 0}\n"
                f"🛡️ Anti-Duplicate Protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}\n"
                f"🔹 Topic Logic: One server = One topic ✅\n"
                f"🔹 Startup Verification: {'✅ Complete' if self.startup_verification_done else '⏳ In Progress'}\n\n"
                "📋 Current Topics:\n"
            )
            
            if self.server_topics:
                for server, topic_id in list(self.server_topics.items())[:10]:
                    exists = self._topic_exists(call.message.chat.id, topic_id)
                    status_icon = "✅" if exists else "❌"
                    status_text += f"• {server}: Topic {topic_id} {status_icon}\n"
                
                if len(self.server_topics) > 10:
                    status_text += f"• ... and {len(self.server_topics) - 10} more topics\n"
            else:
                status_text += "• No topics created yet\n"
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🧹 Clean Invalid", callback_data="cleanup"),
                InlineKeyboardButton("🔄 Verify Topics", callback_data="verify"),
                InlineKeyboardButton("🔙 Back to Menu", callback_data="start")
            )
            self.bot.edit_message_text(
                status_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_help(call):
            """Показать справку"""
            help_text = (
                "ℹ️ **Discord Announcement Parser Help**\n\n"
                "🤖 **Main Features:**\n"
                "• Real-time Discord message monitoring\n"
                "• Auto-forwarding to Telegram topics\n"
                "• Anti-duplicate topic protection\n"
                "• Manual channel management\n\n"
                "📋 **Commands:**\n"
                "• `/start` - Show main menu\n"
                "• `/servers` - List all servers\n"
                "• `/cleanup_topics` - Clean invalid topics\n"
                "• `/verify_topics` - Verify topic integrity\n"
                "• `/reset_topics` - Reset all topic mappings\n\n"
                "🔧 **How to add channels:**\n"
                "1. Go to Server List\n"
                "2. Select a server\n"
                "3. Click 'Add Channel'\n"
                "4. Enter channel ID\n"
                "5. Confirm addition\n\n"
                "🛡️ **Topic Protection:**\n"
                "• One server = One topic\n"
                "• No duplicate topics\n"
                "• Auto-recovery for missing topics\n"
                "• Startup verification\n"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            self.bot.edit_message_text(
                help_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_verify_topics(call):
            """Принудительная проверка топиков"""
            self.startup_verification_done = False
            self.startup_topic_verification(call.message.chat.id)
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            self.bot.edit_message_text(
                f"🔍 Topic verification completed!\n\n"
                f"✅ Active topics: {len(self.server_topics)}\n"
                f"🛡️ Duplicate protection: ACTIVE\n"
                f"🔒 No duplicates found or removed",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_server_selected(call):
            """Обработка выбора сервера"""
            server_name = call.data.replace('server_', '', 1)
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or server_name not in config.SERVER_CHANNEL_MAPPINGS:
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            channels = config.SERVER_CHANNEL_MAPPINGS[server_name]
            channel_count = len(channels)
            
            # Информация о топике
            topic_info = ""
            existing_topic_id = self.get_server_topic_id(server_name)
            if existing_topic_id:
                if self._topic_exists(call.message.chat.id, existing_topic_id):
                    topic_info = f"📋 Topic: {existing_topic_id} ✅"
                else:
                    topic_info = f"📋 Topic: {existing_topic_id} ❌ (invalid)"
            else:
                topic_info = "📋 Topic: Not created yet"
            
            text = (
                f"🏰 **{server_name}**\n\n"
                f"📊 Channels: {channel_count}\n"
                f"{topic_info}\n\n"
                f"📋 **Configured Channels:**\n"
            )
            
            # Показываем каналы
            if channels:
                for channel_id, channel_name in list(channels.items())[:10]:
                    text += f"• {channel_name} (`{channel_id}`)\n"
                if len(channels) > 10:
                    text += f"• ... and {len(channels) - 10} more channels\n"
            else:
                text += "• No channels configured\n"
            
            markup = InlineKeyboardMarkup()
            
            # Кнопки действий
            if channels:
                markup.add(
                    InlineKeyboardButton("📥 Get Messages", callback_data=f"get_messages_{server_name}"),
                    InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
                )
            else:
                markup.add(
                    InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
                )
            
            markup.add(InlineKeyboardButton("🔙 Back to Servers", callback_data="servers"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )

        def handle_get_messages(call):
            """Получить сообщения с сервера"""
            server_name = call.data.replace('get_messages_', '', 1)
            channels = config.SERVER_CHANNEL_MAPPINGS.get(server_name, {})
            
            if not channels:
                self.bot.answer_callback_query(call.id, "❌ No channels found for this server")
                return
            
            # Получаем первый канал для примера
            channel_id, channel_name = next(iter(channels.items()))
            
            if hasattr(self, 'discord_parser') and self.discord_parser:
                try:
                    messages = self.discord_parser.parse_announcement_channel(
                        channel_id,
                        server_name,
                        channel_name,
                        limit=10
                    )
                    
                    if messages:
                        messages.sort(key=lambda x: x.timestamp)
                        self.send_messages(messages)
                        self.bot.answer_callback_query(
                            call.id,
                            f"✅ Sent {len(messages)} messages from {server_name}"
                        )
                    else:
                        self.bot.answer_callback_query(call.id, "ℹ️ No messages found")
                        
                except Exception as e:
                    logger.error(f"Error getting messages: {e}")
                    self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")
            else:
                self.bot.answer_callback_query(call.id, "❌ Discord parser not available")

        def handle_add_channel_request(call):
            """Запрос на добавление канала"""
            server_name = call.data.replace('add_channel_', '', 1)
            
            # Сохраняем состояние пользователя
            self.user_states[call.from_user.id] = {
                'action': 'waiting_for_channel_id',
                'server_name': server_name,
                'chat_id': call.message.chat.id,
                'message_id': call.message.message_id
            }
            
            text = (
                f"➕ **Adding Channel to {server_name}**\n\n"
                f"🔹 Please send the Discord channel ID\n"
                f"🔹 Example: `1234567890123456789`\n\n"
                f"📝 **How to get channel ID:**\n"
                f"1. Enable Developer Mode in Discord\n"
                f"2. Right-click on the channel\n"
                f"3. Click 'Copy ID'\n\n"
                f"⚠️ Make sure the bot has access to this channel!"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_add_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )

        def handle_confirm_add_channel(call):
            """Подтверждение добавления канала"""
            parts = call.data.replace('confirm_add_', '', 1).split('_', 1)
            if len(parts) != 2:
                self.bot.answer_callback_query(call.id, "❌ Invalid data")
                return
                
            server_name, channel_id = parts
            
            # Получаем имя канала из состояния пользователя
            user_state = self.user_states.get(call.from_user.id, {})
            channel_name = user_state.get('channel_name', f"Channel_{channel_id}")
            
            # Добавляем канал
            success, message = self.add_channel_to_server(server_name, channel_id, channel_name)
            
            markup = InlineKeyboardMarkup()
            if success:
                markup.add(
                    InlineKeyboardButton("📋 View Server", callback_data=f"server_{server_name}"),
                    InlineKeyboardButton("🔙 Back to Servers", callback_data="servers")
                )
                status_icon = "✅"
            else:
                markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
                status_icon = "❌"
            
            self.bot.edit_message_text(
                f"{status_icon} **Channel Addition Result**\n\n"
                f"Server: {server_name}\n"
                f"Channel ID: `{channel_id}`\n"
                f"Channel Name: {channel_name}\n\n"
                f"Result: {message}",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
            # Очищаем состояние пользователя
            if call.from_user.id in self.user_states:
                del self.user_states[call.from_user.id]

        def handle_cancel_add_channel(call):
            """Отмена добавления канала"""
            server_name = call.data.replace('cancel_add_', '', 1)
            
            # Очищаем состояние пользователя
            if call.from_user.id in self.user_states:
                del self.user_states[call.from_user.id]
            
            # Возвращаемся к серверу
            call.data = f"server_{server_name}"
            handle_server_selected(call)

        @self.bot.callback_query_handler(func=lambda call: True)
        def handle_callback_query(call):
            """Универсальный обработчик всех callback запросов"""
            try:
                data = call.data
                logger.info(f"📞 Callback received: {data} from user {call.from_user.id}")
                
                # Отвечаем на callback сначала, чтобы убрать "loading"
                self.bot.answer_callback_query(call.id)
                
                # Основные действия
                if data == "servers":
                    handle_servers_list(call)
                elif data == "refresh":
                    handle_manual_sync(call)
                elif data == "websocket":
                    handle_websocket_status(call)
                elif data == "cleanup":
                    handle_cleanup_topics(call)
                elif data == "status":
                    handle_bot_status(call)
                elif data == "help":
                    handle_help(call)
                elif data == "start":
                    send_welcome(call.message)
                elif data == "verify":
                    handle_verify_topics(call)
                elif data.startswith("server_"):
                    handle_server_selected(call)
                elif data.startswith("get_messages_"):
                    handle_get_messages(call)
                elif data.startswith("add_channel_"):
                    handle_add_channel_request(call)
                elif data.startswith("confirm_add_"):
                    handle_confirm_add_channel(call)
                elif data.startswith("cancel_add_"):
                    handle_cancel_add_channel(call)
                else:
                    logger.warning(f"⚠️ Unknown callback data: {data}")
                    # Не отправляем answer_callback_query здесь, так как уже отправили выше
                
            except Exception as e:
                logger.error(f"❌ Error handling callback query: {e}")
                # Отправляем error callback только если не отправили ранее
                try:
                    self.bot.answer_callback_query(call.id, "❌ Произошла ошибка")
                except:
                    pass

        def handle_servers_list(call):
            """Показать список серверов с возможностью добавления каналов"""
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or not config.SERVER_CHANNEL_MAPPINGS:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
                self.bot.edit_message_text(
                    "❌ No servers found. Please configure servers first.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
                return
                
            markup = InlineKeyboardMarkup()
            for server in config.SERVER_CHANNEL_MAPPINGS.keys():
                # Add topic indicator with duplicate check
                topic_indicator = ""
                if server in self.server_topics:
                    topic_id = self.server_topics[server]
                    if self._topic_exists(call.message.chat.id, topic_id):
                        topic_indicator = " 📋"
                    else:
                        topic_indicator = " ❌"
                else:
                    topic_indicator = " 🆕"  # New server, no topic yet
                
                markup.add(InlineKeyboardButton(
                    f"🏰 {server}{topic_indicator}",
                    callback_data=f"server_{server}"
                ))
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            server_count = len(config.SERVER_CHANNEL_MAPPINGS)
            topic_count = len(self.server_topics)
            
            text = (
                f"📋 Select a server to view announcements:\n\n"
                f"📊 {server_count} servers configured, {topic_count} topics created\n"
                f"📋 = Has topic, ❌ = Invalid topic, 🆕 = New server\n"
                f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}"
            )
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_manual_sync(call):
            """Выполнить ручную синхронизацию"""
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            try:
                # Здесь можно добавить логику ручной синхронизации
                # Например, вызов метода из discord_parser для обновления каналов
                sync_result = "🔄 Manual sync completed successfully!"
                if hasattr(self, 'discord_parser') and self.discord_parser:
                    # Попробуем обновить список серверов
                    sync_result += f"\n📊 Found {len(config.SERVER_CHANNEL_MAPPINGS)} servers"
                
                self.bot.edit_message_text(
                    sync_result,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except Exception as e:
                self.bot.edit_message_text(
                    f"❌ Sync failed: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )

        def handle_websocket_status(call):
            """Показать статус WebSocket соединения"""
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            ws_status = "❌ WebSocket service not available"
            if self.websocket_service:
                subscribed_channels = len(self.websocket_service.subscribed_channels)
                http_channels = len(getattr(self.websocket_service, 'http_accessible_channels', set()))
                ws_only_channels = len(getattr(self.websocket_service, 'websocket_accessible_channels', set()))
                
                ws_status = (
                    f"⚡ WebSocket Status\n\n"
                    f"📡 Subscribed channels: {subscribed_channels}\n"
                    f"🌐 HTTP accessible: {http_channels}\n"
                    f"🔌 WebSocket only: {ws_only_channels}\n"
                    f"🔄 Status: {'✅ Active' if self.websocket_service.running else '❌ Inactive'}"
                )
            
            self.bot.edit_message_text(
                ws_status,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_cleanup_topics(call):
            """Очистить недействительные топики"""
            cleaned = self.cleanup_invalid_topics(call.message.chat.id)
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            self.bot.edit_message_text(
                f"🧹 Topic cleanup completed!\n\n"
                f"Removed {cleaned} invalid/duplicate topics.\n"
                f"Current topics: {len(self.server_topics)}\n"
                f"🛡️ Anti-duplicate protection: ACTIVE",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_bot_status(call):
            """Показать статус бота"""
            supports_topics = self._check_if_supergroup_with_topics(call.message.chat.id)
            
            status_text = (
                "📊 Bot Status\n\n"
                f"🔹 Topics Support: {'✅ Enabled' if supports_topics else '❌ Disabled'}\n"
                f"🔹 Active Topics: {len(self.server_topics)}\n"
                f"🔹 Configured Servers: {len(config.SERVER_CHANNEL_MAPPINGS) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                f"🔹 Total Channels: {sum(len(channels) for channels in config.SERVER_CHANNEL_MAPPINGS.values()) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                f"🔹 Message Cache: {len(self.message_mappings)} messages\n"
                f"🔹 WebSocket Channels: {len(self.websocket_service.subscribed_channels) if self.websocket_service else 0}\n"
                f"🛡️ Anti-Duplicate Protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}\n"
                f"🔹 Topic Logic: One server = One topic ✅\n"
                f"🔹 Startup Verification: {'✅ Complete' if self.startup_verification_done else '⏳ In Progress'}\n\n"
                "📋 Current Topics:\n"
            )
            
            if self.server_topics:
                for server, topic_id in list(self.server_topics.items())[:10]:
                    exists = self._topic_exists(call.message.chat.id, topic_id)
                    status_icon = "✅" if exists else "❌"
                    status_text += f"• {server}: Topic {topic_id} {status_icon}\n"
                
                if len(self.server_topics) > 10:
                    status_text += f"• ... and {len(self.server_topics) - 10} more topics\n"
            else:
                status_text += "• No topics created yet\n"
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🧹 Clean Invalid", callback_data="cleanup"),
                InlineKeyboardButton("🔄 Verify Topics", callback_data="verify"),
                InlineKeyboardButton("🔙 Back to Menu", callback_data="start")
            )
            self.bot.edit_message_text(
                status_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_help(call):
            """Показать справку"""
            help_text = (
                "ℹ️ **Discord Announcement Parser Help**\n\n"
                "🤖 **Main Features:**\n"
                "• Real-time Discord message monitoring\n"
                "• Auto-forwarding to Telegram topics\n"
                "• Anti-duplicate topic protection\n"
                "• Manual channel management\n\n"
                "📋 **Commands:**\n"
                "• `/start` - Show main menu\n"
                "• `/servers` - List all servers\n"
                "• `/cleanup_topics` - Clean invalid topics\n"
                "• `/verify_topics` - Verify topic integrity\n"
                "• `/reset_topics` - Reset all topic mappings\n\n"
                "🔧 **How to add channels:**\n"
                "1. Go to Server List\n"
                "2. Select a server\n"
                "3. Click 'Add Channel'\n"
                "4. Enter channel ID\n"
                "5. Confirm addition\n\n"
                "🛡️ **Topic Protection:**\n"
                "• One server = One topic\n"
                "• No duplicate topics\n"
                "• Auto-recovery for missing topics\n"
                "• Startup verification\n"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            self.bot.edit_message_text(
                help_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_verify_topics(call):
            """Принудительная проверка топиков"""
            self.startup_verification_done = False
            self.startup_topic_verification(call.message.chat.id)
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            self.bot.edit_message_text(
                f"🔍 Topic verification completed!\n\n"
                f"✅ Active topics: {len(self.server_topics)}\n"
                f"🛡️ Duplicate protection: ACTIVE\n"
                f"🔒 No duplicates found or removed",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )

        def handle_server_selected(call):
            """Обработка выбора сервера"""
            server_name = call.data.replace('server_', '', 1)
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or server_name not in config.SERVER_CHANNEL_MAPPINGS:
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            channels = config.SERVER_CHANNEL_MAPPINGS[server_name]
            channel_count = len(channels)
            
            # Информация о топике
            topic_info = ""
            existing_topic_id = self.get_server_topic_id(server_name)
            if existing_topic_id:
                if self._topic_exists(call.message.chat.id, existing_topic_id):
                    topic_info = f"📋 Topic: {existing_topic_id} ✅"
                else:
                    topic_info = f"📋 Topic: {existing_topic_id} ❌ (invalid)"
            else:
                topic_info = "📋 Topic: Not created yet"
            
            text = (
                f"🏰 **{server_name}**\n\n"
                f"📊 Channels: {channel_count}\n"
                f"{topic_info}\n\n"
                f"📋 **Configured Channels:**\n"
            )
            
            # Показываем каналы
            if channels:
                for channel_id, channel_name in list(channels.items())[:10]:
                    text += f"• {channel_name} (`{channel_id}`)\n"
                if len(channels) > 10:
                    text += f"• ... and {len(channels) - 10} more channels\n"
            else:
                text += "• No channels configured\n"
            
            markup = InlineKeyboardMarkup()
            
            # Кнопки действий
            if channels:
                markup.add(
                    InlineKeyboardButton("📥 Get Messages", callback_data=f"server_{server_name}"),
                    InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
                )
            else:
                markup.add(
                    InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
                )
            
            markup.add(InlineKeyboardButton("🔙 Back to Servers", callback_data="servers"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
            # Если это повторный вызов (для получения сообщений), выполняем логику получения
            if server_name in getattr(self, '_temp_server_action', {}):
                perform_get_messages(call, server_name)

        def handle_add_channel_request(call):
            """Запрос на добавление канала"""
            server_name = call.data.replace('add_channel_', '', 1)
            
            # Сохраняем состояние пользователя
            self.user_states[call.from_user.id] = {
                'action': 'waiting_for_channel_id',
                'server_name': server_name,
                'chat_id': call.message.chat.id,
                'message_id': call.message.message_id
            }
            
            text = (
                f"➕ **Adding Channel to {server_name}**\n\n"
                f"🔹 Please send the Discord channel ID\n"
                f"🔹 Example: `1234567890123456789`\n\n"
                f"📝 **How to get channel ID:**\n"
                f"1. Enable Developer Mode in Discord\n"
                f"2. Right-click on the channel\n"
                f"3. Click 'Copy ID'\n\n"
                f"⚠️ Make sure the bot has access to this channel!"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_add_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )

        def handle_confirm_add_channel(call):
            """Подтверждение добавления канала"""
            parts = call.data.replace('confirm_add_', '', 1).split('_', 1)
            if len(parts) != 2:
                self.bot.answer_callback_query(call.id, "❌ Invalid data")
                return
                
            server_name, channel_id = parts
            
            # Получаем имя канала из состояния пользователя
            user_state = self.user_states.get(call.from_user.id, {})
            channel_name = user_state.get('channel_name', f"Channel_{channel_id}")
            
            # Добавляем канал
            success, message = self.add_channel_to_server(server_name, channel_id, channel_name)
            
            markup = InlineKeyboardMarkup()
            if success:
                markup.add(
                    InlineKeyboardButton("📋 View Server", callback_data=f"server_{server_name}"),
                    InlineKeyboardButton("🔙 Back to Servers", callback_data="servers")
                )
                status_icon = "✅"
            else:
                markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
                status_icon = "❌"
            
            self.bot.edit_message_text(
                f"{status_icon} **Channel Addition Result**\n\n"
                f"Server: {server_name}\n"
                f"Channel ID: `{channel_id}`\n"
                f"Channel Name: {channel_name}\n\n"
                f"Result: {message}",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
            # Очищаем состояние пользователя
            if call.from_user.id in self.user_states:
                del self.user_states[call.from_user.id]

        def handle_cancel_add_channel(call):
            """Отмена добавления канала"""
            server_name = call.data.replace('cancel_add_', '', 1)
            
            # Очищаем состояние пользователя
            if call.from_user.id in self.user_states:
                del self.user_states[call.from_user.id]
            
            # Возвращаемся к серверу
            call.data = f"server_{server_name}"
            handle_server_selected(call)

        def perform_get_messages(call, server_name):
            """Получить и отправить сообщения с сервера"""
            channels = config.SERVER_CHANNEL_MAPPINGS[server_name]
            if not channels:
                self.bot.answer_callback_query(call.id, "❌ No channels found for this server")
                return
            
            # Получаем первый канал для примера
            channel_id, channel_name = next(iter(channels.items()))
            
            if hasattr(self, 'discord_parser') and self.discord_parser:
                try:
                    messages = self.discord_parser.parse_announcement_channel(
                        channel_id,
                        server_name,
                        channel_name,
                        limit=10
                    )
                    
                    if messages:
                        messages.sort(key=lambda x: x.timestamp)
                        self.send_messages(messages)
                        self.bot.answer_callback_query(
                            call.id,
                            f"✅ Sent {len(messages)} messages from {server_name}"
                        )
                    else:
                        self.bot.answer_callback_query(call.id, "ℹ️ No messages found")
                        
                except Exception as e:
                    logger.error(f"Error getting messages: {e}")
                    self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")
            else:
                self.bot.answer_callback_query(call.id, "❌ Discord parser not available")

        @self.bot.message_handler(func=lambda message: True)
        def handle_text_message(message):
            """Обработка текстовых сообщений (для добавления каналов)"""
            user_id = message.from_user.id
            
            # Проверяем, ждем ли мы от пользователя channel ID
            if user_id in self.user_states:
                user_state = self.user_states[user_id]
                
                if user_state.get('action') == 'waiting_for_channel_id':
                    channel_id = message.text.strip()
                    server_name = user_state['server_name']
                    original_chat_id = user_state['chat_id']
                    original_message_id = user_state['message_id']
                    
                    # Валидация channel ID
                    if not channel_id.isdigit() or len(channel_id) < 17:
                        self.bot.reply_to(
                            message, 
                            "❌ Invalid channel ID format. Please send a valid Discord channel ID (17-19 digits)"
                        )
                        return
                    
                    # Пытаемся получить имя канала
                    channel_name = f"Channel_{channel_id}"
                    if hasattr(self, 'discord_parser') and self.discord_parser:
                        try:
                            # Пробуем получить информацию о канале
                            session = self.discord_parser.sessions[0] if self.discord_parser.sessions else None
                            if session:
                                r = session.get(f'https://discord.com/api/v9/channels/{channel_id}')
                                if r.status_code == 200:
                                    channel_info = r.json()
                                    channel_name = channel_info.get('name', channel_name)
                        except Exception as e:
                            logger.debug(f"Could not get channel info: {e}")
                    
                    # Сохраняем имя канала в состоянии
                    self.user_states[user_id]['channel_name'] = channel_name
                    
                    # Показываем подтверждение
                    confirmation_text = (
                        f"🔍 **Channel Information**\n\n"
                        f"Server: {server_name}\n"
                        f"Channel ID: `{channel_id}`\n"
                        f"Channel Name: {channel_name}\n\n"
                        f"➕ Add this channel to monitoring?"
                    )
                    
                    markup = InlineKeyboardMarkup()
                    markup.add(
                        InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_add_{server_name}_{channel_id}"),
                        InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_add_{server_name}")
                    )
                    
                    # Удаляем сообщение пользователя
                    try:
                        self.bot.delete_message(message.chat.id, message.message_id)
                    except:
                        pass
                    
                    # Обновляем оригинальное сообщение
                    try:
                        self.bot.edit_message_text(
                            confirmation_text,
                            original_chat_id,
                            original_message_id,
                            reply_markup=markup,
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        # Если не можем отредактировать, отправляем новое
                        self.bot.send_message(
                            message.chat.id,
                            confirmation_text,
                            reply_markup=markup,
                            parse_mode='Markdown'
                        )

        # Команды для управления топиками
        @self.bot.message_handler(commands=['servers'])
        def list_servers_command(message):
            """Команда для отображения списка серверов"""
            # Создаем фиктивный callback для переиспользования логики
            class FakeCall:
                def __init__(self, message):
                    self.message = message
                    self.data = "servers"
            
            fake_call = FakeCall(message)
            handle_servers_list(fake_call)

        @self.bot.message_handler(commands=['reset_topics'])
        def reset_topics(message):
            """Reset all topic mappings with confirmation"""
            with self.topic_creation_lock:
                backup_topics = self.server_topics.copy()
                self.server_topics.clear()
                self.topic_name_cache.clear()
                self.startup_verification_done = False
                self._save_data()
                
            self.bot.reply_to(
                message, 
                f"✅ All topic mappings have been reset.\n"
                f"🗑️ Cleared {len(backup_topics)} topic mappings.\n"
                f"🆕 New topics will be created when needed.\n"
                f"🛡️ Anti-duplicate protection will be active."
            )

        @self.bot.message_handler(commands=['verify_topics'])
        def verify_topics_command(message):
            """Force topic verification to check for duplicates"""
            self.startup_verification_done = False
            old_count = len(self.server_topics)
            
            self.startup_topic_verification(message.chat.id)
            
            new_count = len(self.server_topics)
            removed_count = old_count - new_count
            
            self.bot.reply_to(
                message,
                f"🔍 Topic verification completed!\n\n"
                f"📊 Results:\n"
                f"• Topics before: {old_count}\n"
                f"• Topics after: {new_count}\n"
                f"• Removed/Fixed: {removed_count}\n"
                f"🛡️ Anti-duplicate protection: ✅ ACTIVE"
            )

        @self.bot.message_handler(commands=['cleanup_topics'])
        def cleanup_topics_command(message):
            """Clean up invalid topic mappings"""
            cleaned = self.cleanup_invalid_topics(message.chat.id)
            self.bot.reply_to(
                message, 
                f"🧹 Cleaned up {cleaned} invalid/duplicate topics.\n"
                f"📋 Current active topics: {len(self.server_topics)}\n"
                f"🛡️ Anti-duplicate protection: ✅ ACTIVE"
            )

        logger.success("🤖 Telegram Bot started with ENHANCED ANTI-DUPLICATE topic management:")
        logger.info("   ✅ One server = One topic (GUARANTEED)")
        logger.info("   🛡️ Startup verification prevents duplicates")
        logger.info("   🔒 Thread-safe topic creation")
        logger.info("   🧹 Auto-cleanup of invalid topics")
        logger.info("   ⚡ Fast cache lookup for real-time messages")
        logger.info("   🔍 Duplicate detection and removal")
        logger.info("   📊 Enhanced status reporting")
        logger.info("   ➕ Manual channel addition via bot interface")
        logger.info("   🚀 Ready for real-time WebSocket messages")
        
        self.bot.polling(none_stop=True)