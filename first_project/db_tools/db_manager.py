#конфигурация
import os
import psycopg2
from dotenv import load_dotenv
# логирование
import logging
# алхимия
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Boolean, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from datetime import datetime
from sqlalchemy import inspect

#подгрузка конфиг файлов из dotevn
load_dotenv()

# настройки логера
logging.basicConfig(
    level=logging.DEBUG,#задаем уровень отображения логгеров(здесь от дебага и выше)
    format='[%(asctime)s] #%(levelname)-8s %(filename)s:'
           '%(lineno)d - %(name)s - %(message)s'
)


logger = logging.getLogger(__name__)


class DynamicModelManager:
    def __init__(self, db_url: str):
        """
        Инициализация менеджера динамических моделей
        """
        self.db_url = db_url
        self.engine = create_engine(db_url, echo=False)
        self.Base = declarative_base()
        self.Session = sessionmaker(bind=self.engine)
        self.created_models = {}
        
        logger.info(f"Initialized DynamicModelManager for database: {db_url}")

    def _table_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица в базе данных"""
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def _create_model_from_existing_table(self, table_name: str):
        """Создает модель из существующей таблицы через рефлексию"""
        try:
            # Создаем метаданные и отражаем таблицу из базы
            metadata = MetaData()
            existing_table = Table(table_name, metadata, autoload_with=self.engine)
            
            # Создаем атрибуты для класса модели
            attributes = {
                '__tablename__': table_name,
                '__table__': existing_table
            }
            
            # Динамически создаем класс модели
            model_class = type(
                f'{table_name.capitalize()}',
                (self.Base,),
                attributes
            )
            
            self.created_models[table_name] = model_class
            logger.info(f"Created model for existing table: {table_name}")
            
            return model_class
            
        except Exception as e:
            logger.error(f"Error creating model from existing table {table_name}: {str(e)}")
            raise

    def create_dynamic_model(self, table_name: str, columns_config: dict):
        """
        Динамически создает модель и таблицу в БД
        """
        try:
            # Проверяем в базе данных
            if self._table_exists(table_name):
                logger.warning(f"Table {table_name} already exists in database")
                # Создаем модель из существующей таблицы
                return self._create_model_from_existing_table(table_name)
            
            # Проверяем в локальном словаре
            if table_name in self.created_models:
                logger.warning(f"Model {table_name} already created in this session")
                return self.created_models[table_name]

            # Создаем атрибуты для класса модели
            attributes = {
                '__tablename__': table_name,
                'id': Column(Integer, primary_key=True, autoincrement=True)
            }

            # Добавляем колонки на основе конфигурации
            for column_name, column_config in columns_config.items():
                column_type, column_options = column_config
                attributes[column_name] = self._create_column(column_type, column_options)

            # Динамически создаем класс модели
            model_class = type(
                f'{table_name.capitalize()}',
                (self.Base,),
                attributes
            )

            # Создаем таблицу в БД
            self.Base.metadata.create_all(self.engine, tables=[model_class.__table__])
            
            self.created_models[table_name] = model_class
            logger.info(f"Successfully created table '{table_name}' with columns: {list(columns_config.keys())}")
            
            return model_class

        except Exception as e:
            logger.error(f"Error creating dynamic model {table_name}: {str(e)}")
            raise

    def _create_column(self, column_type: str, options: dict):
        """Создает колонку на основе типа и опций"""
        type_mapping = {
            'integer': Integer,
            'string': String,
            'text': Text,
            'datetime': DateTime,
            'float': Float,
            'boolean': Boolean
        }
        # проверка на соответстиве типам данных бд
        if column_type not in type_mapping:
            raise ValueError(f"Unsupported column type: {column_type}")

        sqlalchemy_type = type_mapping[column_type]
        
        # Обрабатываем специальные параметры для String
        if column_type == 'string':
            length = options.get('length', 255)
            return Column(sqlalchemy_type(length), **self._clean_options(options, ['length']))
        
        return Column(sqlalchemy_type, **options)

    def _clean_options(self, options: dict, exclude_keys: list):
        """Удаляет специфичные ключи из опций"""
        return {k: v for k, v in options.items() if k not in exclude_keys}

    def get_session(self):
        """Возвращает сессию для работы с БД"""
        return self.Session()

    def insert_data(self, table_name: str, data: dict):
        """Вставляет данные в указанную таблицу"""
        try:
            if not self._table_exists(table_name):
                raise ValueError(f"Table {table_name} not found. Create it first.")

            # Если модель еще не создана в этом экземпляре - создаем ее
            if table_name not in self.created_models:
                logger.warning(f"Model for table {table_name} not found in cache. Creating model from existing table.")
                self._create_model_from_existing_table(table_name)

            model_class = self.created_models[table_name]
            session = self.get_session()
            
            instance = model_class(**data)
            session.add(instance)
            session.commit()
            
            logger.info(f"Successfully inserted data into {table_name}")
            return instance
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении данных в {table_name}: {str(e)}")
            raise

    def get_all_data(self, table_name: str):
        """Получает все данные из таблицы"""
        try:
            if not self._table_exists(table_name):
                raise ValueError(f"Table {table_name} not found")

            # Если модель еще не создана - создаем ее
            if table_name not in self.created_models:
                self._create_model_from_existing_table(table_name)

            model_class = self.created_models[table_name]
            session = self.get_session()
            
            return session.query(model_class).all()
            
        except Exception as e:
            logger.error(f"Error fetching data from {table_name}: {str(e)}")
            raise

    def drop_table(self, table_name: str):
        """Удаляет таблицу из БД"""
        try:
            if self._table_exists(table_name):
                if table_name in self.created_models:
                    model_class = self.created_models[table_name]
                    model_class.__table__.drop(self.engine)
                else:
                    # Если модели нет в кеше, но таблица существует
                    metadata = MetaData()
                    table = Table(table_name, metadata, autoload_with=self.engine)
                    table.drop(self.engine)
                
                if table_name in self.created_models:
                    del self.created_models[table_name]
                    
                logger.info(f"Table {table_name} dropped successfully")
            else:
                raise ValueError(f"Table {table_name} not found")
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {str(e)}")
            raise

    def close(self):
        """Закрывает соединение с БД"""
        self.engine.dispose()