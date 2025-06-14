import os
from dotenv import load_dotenv

class Config:
    def __init__(self):
        load_dotenv()
        
        # Discord Configuration
        self.DISCORD_TOKENS = [
            t.strip() for t in 
            os.getenv('DISCORD_AUTH_TOKENS', '').split(',') 
            if t.strip()
        ]
        
        # Parsing Configuration
        self.PARSE_TYPE_ALL_CHAT = 1
        self.PARSE_TYPE_ONE_USER = 2
        self.PARSE_TYPE_DELETE_DUPLICATES = 3
        
        # Message Parsing Parameters
        self.MIN_WORDS = 0
        self.TRANSLATE_MESSAGES = False
        self.TRANSLATION_LANGUAGE = 'en'
        
        # Telegram Configuration
        self.TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
        
        # Server/Channel Mappings
        self.SERVER_CHANNEL_MAPPINGS = {}
        
        # Telegram UI Preferences
        self.TELEGRAM_UI_PREFERENCES = {
            'use_topics': True,
            'show_timestamps': True
        }

config = Config()



config.TELEGRAM_CHAT_ID = -1002881735463










