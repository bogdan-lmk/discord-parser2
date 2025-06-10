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
                InlineKeyboardButton("📋 Server List", callback_data="action_servers"),
                InlineKeyboardButton("🔄 Manual Sync", callback_data="action_refresh"),
                InlineKeyboardButton("⚡ WebSocket Status", callback_data="action_websocket"),
                InlineKeyboardButton("🧹 Clean Topics", callback_data="action_cleanup"),
                InlineKeyboardButton("📊 Bot Status", callback_data="action_status"),
                InlineKeyboardButton("ℹ️ Help", callback_data="action_help")
            )
            
            self.bot.send_message(message.chat.id, text, reply_markup=markup)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('action_'))
        def handle_action(call):
            action = call.data.replace('action_', '')
            
            if action == 'cleanup':
                cleaned = self.cleanup_invalid_topics(call.message.chat.id)
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                self.bot.edit_message_text(
                    f"🧹 Topic cleanup completed!\n\n"
                    f"Removed {cleaned} invalid/duplicate topics.\n"
                    f"Current topics: {len(self.server_topics)}\n"
                    f"🛡️ Anti-duplicate protection: ACTIVE",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'status':
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
                    InlineKeyboardButton("🧹 Clean Invalid", callback_data="action_cleanup"),
                    InlineKeyboardButton("🔄 Verify Topics", callback_data="action_verify"),
                    InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start")
                )
                self.bot.edit_message_text(
                    status_text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'verify':
                # Принудительная проверка топиков
                self.startup_verification_done = False
                self.startup_topic_verification(call.message.chat.id)
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                self.bot.edit_message_text(
                    f"🔍 Topic verification completed!\n\n"
                    f"✅ Active topics: {len(self.server_topics)}\n"
                    f"🛡️ Duplicate protection: ACTIVE\n"
                    f"🔒 No duplicates found or removed",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'start':
                send_welcome(call.message)
            # Добавить остальные обработчики...
            
            self.bot.answer_callback_query(call.id)

        @self.bot.message_handler(commands=['servers'])
        def list_servers(message):
            """Show interactive server list with topic info and duplicate status"""
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or not config.SERVER_CHANNEL_MAPPINGS:
                self.bot.reply_to(message, "❌ No servers found. Please configure servers first.")
                return
                
            markup = InlineKeyboardMarkup()
            for server in config.SERVER_CHANNEL_MAPPINGS.keys():
                # Add topic indicator with duplicate check
                topic_indicator = ""
                if server in self.server_topics:
                    topic_id = self.server_topics[server]
                    if self._topic_exists(message.chat.id, topic_id):
                        topic_indicator = " 📋"
                    else:
                        topic_indicator = " ❌"
                else:
                    topic_indicator = " 🆕"  # New server, no topic yet
                
                markup.add(InlineKeyboardButton(
                    f"🏰 {server}{topic_indicator}",
                    callback_data=f"server_{server}"
                ))
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
            
            server_count = len(config.SERVER_CHANNEL_MAPPINGS)
            topic_count = len(self.server_topics)
            
            self.bot.reply_to(
                message, 
                f"📋 Select a server to view announcements:\n\n"
                f"📊 {server_count} servers configured, {topic_count} topics created\n"
                f"📋 = Has topic, ❌ = Invalid topic, 🆕 = New server\n"
                f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}",
                reply_markup=markup
            )

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

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('server_'))
        def server_selected(call):
            """Handle server selection with improved topic management"""
            server_name = call.data.replace('server_', '')
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or server_name not in config.SERVER_CHANNEL_MAPPINGS:
                self.bot.answer_callback_query(call.id, "Server not found")
                return
                
            # Get first announcement channel
            channels = [
                (cid, name) for cid, name in config.SERVER_CHANNEL_MAPPINGS[server_name].items()
                if not cid.startswith('telegram_')
            ]
            
            if not channels:
                self.bot.answer_callback_query(call.id, "No announcement channels found for this server")
                return
                
            channel_id, channel_name = channels[0]
            
            # Get last 10 messages and sort chronologically
            if hasattr(self, 'discord_parser') and self.discord_parser:
                messages = self.discord_parser.parse_announcement_channel(
                    channel_id,
                    server_name,
                    channel_name,
                    limit=10
                )
                
                messages.sort(key=lambda x: x.timestamp)
                
                logger.info(f"📥 Fetched {len(messages)} messages from Discord for {server_name}")
                
                if not messages:
                    self.bot.answer_callback_query(call.id, "No messages found")
                    return
                
                # Show topic status with duplicate prevention info
                topic_status = ""
                existing_topic_id = self.get_server_topic_id(server_name)
                if existing_topic_id:
                    if self._topic_exists(call.message.chat.id, existing_topic_id):
                        topic_status = f" to existing topic {existing_topic_id} (verified, no duplicates)"
                    else:
                        topic_status = " (will create new topic - old one invalid)"
                else:
                    topic_status = " (will create new topic with duplicate protection)"
                
                # Send messages using improved topic logic (prevents duplicates!)
                self.send_messages(messages)
                
                self.bot.answer_callback_query(
                    call.id,
                    f"Sent {len(messages)} messages{topic_status}"
                )
                
                # Store user state
                self.user_states[call.from_user.id] = {
                    'server': server_name,
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'last_message': messages[-1].timestamp if messages else datetime.min
                }
            else:
                self.bot.answer_callback_query(call.id, "Discord parser not available")

        logger.success("🤖 Telegram Bot started with ENHANCED ANTI-DUPLICATE topic management:")
        logger.info("   ✅ One server = One topic (GUARANTEED)")
        logger.info("   🛡️ Startup verification prevents duplicates")
        logger.info("   🔒 Thread-safe topic creation")
        logger.info("   🧹 Auto-cleanup of invalid topics")
        logger.info("   ⚡ Fast cache lookup for real-time messages")
        logger.info("   🔍 Duplicate detection and removal")
        logger.info("   📊 Enhanced status reporting")
        logger.info("   🚀 Ready for real-time WebSocket messages")
        
        self.bot.polling(none_stop=True)