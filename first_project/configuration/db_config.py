import os
from dotenv import load_dotenv

load_dotenv()


DB_CONFIG = {
        'user': os.getenv('DATABASE_USER'),
        'password': os.getenv('DATABASE_PASSWORD'),
        'host': os.getenv('DATABASE_HOST'),
        'port': os.getenv('DATABASE_PORT'),
        'database': os.getenv('DATABASE_NAME')
    }