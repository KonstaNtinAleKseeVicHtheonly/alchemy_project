from db_tools.alternative import  AlternativeModelManager
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Boolean, MetaData
from db_tools.db_manager import DynamicModelManager
from db_tools.alternative import AlternativeModelManager
from datetime import datetime
import logging
from configuration.db_url_config import _create_db_url # для генерации db_url

# Логгирование
logging.basicConfig(
    level=logging.DEBUG,#задаем уровень отображения логгеров(здесь от дебага и выше)
    format='[%(asctime)s] #%(levelname)-8s %(filename)s:'
           '%(lineno)d - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DBManagerInterface:
    def __init__(self,db_url):
        self.db_manager = AlternativeModelManager(db_url)

    def create_model(self,table_name:str,columns_config:dict):
            logger.info(f'процесс создания таблицы {table_name} запущен')
            db_model = self.db_manager.create_model(table_name,columns_config)
            logger.info(f'процесс создания таблицы {table_name} завершен')
            return db_model
    
    def get_model(self,table_name:str):
         logger.info(f'процесс поиска таблицы {table_name} запущен')
         db_model = self.db_manager._get_model(table_name)
         logger.info(f'процесс поиска таблицы {table_name} завершен')
         return db_model
    
    def create_record(self, table_name: str, data: dict):
        logger.info(f'процесс создани строки в таблице  {table_name} запущен')
        db_record = self.db_manager.create_record(table_name,data)
        logger.info(f'процесс создания строки в таблице {table_name} завершен')
        return db_record
    
    def read(self, table_name: str, record_id: int):
        logger.info(f'процесс чтения строки в таблице  {table_name} запущен')
        db_record = self.db_manager.read(table_name,record_id)
        logger.info(f'процесс чтения строки в таблице {table_name} завершен')
        return db_record
    
    def read_all(self,table_name):
        logger.info(f'процесс чтения всех строк из таблицы {table_name} запущен')
        db_record = self.db_manager.read_all(table_name)
        logger.info(f'процесс чтения всех строк из таблицы {table_name} завершен')
        return db_record
    
    def update(self, table_name: str, record_id: int, data: dict):
        logger.info(f'процесс обнволения строки таблицы {table_name} с  id {record_id }запущен')
        db_record = self.db_manager.update(table_name,record_id,data)
        logger.info(f'процесс обнволения строки таблицы {table_name} с  id {record_id }завершен')
        return db_record
    
    def delete(self, table_name: str, record_id: int):
        logger.info(f'процесс удаления строки таблицы {table_name} с  id {record_id }запущен')
        db_record = self.db_manager.delete(table_name,record_id)
        if db_record:
            logger.info(f'процесс удаления строки таблицы {table_name} с  id {record_id }завершен')
            return db_record
        return logger.info(f'Что то пошло не так при удалении записи из ьаблицы {table_name}')
    
    def __delete_table(self, table_name: str):
        logger.info(f"вы собираетесь удалить таблицу {table_name}, подтвердите действие")
        confirmation = input()
        if confirmation.lower() in ['yes','да','Леха лох']:
            logger.warning(f'процесс удаления таблицы {table_name} запущен')
            db_record = self.db_manager.delete_table(table_name)
            logger.warning(f'процесс удаления таблицы {table_name} завершен ')
            return db_record
        return f"удаление таблицы {table_name} отменено"