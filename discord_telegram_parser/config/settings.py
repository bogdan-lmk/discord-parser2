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



config.TELEGRAM_CHAT_ID = -1002890737800









# Auto-discovered channels
config.SERVER_CHANNEL_MAPPINGS = {
  "EnsoFi": {
    "1230813806981021740": "\ud83d\udea8\ufe31announcement",
    "1230815022293450776": "\ud83d\udd08\ufe31defi-announcement",
    "1350428355211690005": "\ud83d\udce3\ufe31event-announcement",
    "1372897682544132218": "\ud83d\udd08\ufe31defai-announcement",
    "1372899741180104785": "\ud83d\udd08\ufe31elander-announcement"
  },
  "\u0421\u0435\u0440\u0432\u0435\u0440 vikaets": {
    "1381919567638433853": "announcements"
  }
}
