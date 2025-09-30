from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Boolean, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.ext.automap import automap_base
import logging
from dotenv import load_dotenv
#
#подгрузка конфиг файлов из dotevn
load_dotenv()


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _create_db_url(db_brand,db_engine,db_info:dict):
    return f"{db_brand}+{db_engine}://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"


class AlternativeModelManager:
    def __init__(self,database_url,base_model=None):
         self.database_url = database_url
         self.engine = create_engine(database_url,echo=False, pool_pre_ping=True)
         self.Base = base_model or declarative_base()
         self.Session = sessionmaker(bind=self.engine)
         self._models: Dict[str,Any] = {} # кэш уже созданных моделей
         self._metadata = MetaData()

    def _table_exists(self, table_name: str) -> bool:
        """Проверяет существование таблицы в БД"""
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_model(self, table_name: str, columns_config: Dict[str, Any]) -> Any:
        """Динамически создает и возвращает класс модели SQLAlchemy.
        Args:
            table_name: Имя таблицы в базе данных
            columns_config: Словарь конфигурации столбцов {имя: тип}
        Returns:
            Динамически созданный класс модели"""
        try:
            # Проверяем, существует ли таблица в БД
            if self._table_exists(table_name):
                logger.warning(f"Table '{table_name}' already exists in database")
                raise ValueError
            # Базовые атрибуты модели
            attrs = {
                '__tablename__': table_name,
                '__table_args__': {'extend_existing': True}
            }

            # Добавляем автоинкрементный первичный ключ 'id'
            attrs['id'] = Column(Integer, primary_key=True, autoincrement=True)
             # Добавляем остальные столбцы из конфигурации
            for col_name, col_type in columns_config.items():
                attrs[col_name] = Column(col_type)
            # Создаем класс с помощью type
            model_class = type(f'{table_name.title().replace("_", "")}',  # Убираем подчеркивания для имени класса
                (self.Base,),
                attrs)
            # Создаем таблицу в БД
            self.Base.metadata.create_all(self.engine, tables=[model_class.__table__])
            # Кэшируем модель
            self._models[table_name] = model_class
            
            logger.info(f"Successfully created model and table '{table_name}'")
            return model_class
        except Exception as e:
            logger.error(f"Error creating model '{table_name}': {str(e)}")
            raise
        
    def _get_model(self, table_name: str, columns_config: Dict[str, Any] = None) -> Any:
        """
        Упрощенный метод получения модели.
        """
        # 1. Если модель в кэше и таблица существует - используем кэш (оптимизация)
        if table_name in self._models and self._table_exists(table_name):
            return self._models[table_name]
        
        # 2. Если таблица существует - отражаем её
        if self._table_exists(table_name):
            model = self._reflect_existing_table(table_name)
            self._models[table_name] = model  # Кэшируем для будущего использования
            return model
        
        raise ValueError(f"Table '{table_name}' does not exist")
    # страый не рабочий вариант через метка класс type
    # def _reflect_existing_table_type_object(self, table_name: str) -> Any:
        """Создает модель из существующей таблицы через рефлексию"""
        try:
            # Рефлексируем существующую таблицу
            self._metadata.reflect(bind=self.engine, only=[table_name])
            existing_table = self._metadata.tables[table_name]
            
            # Создаем атрибуты для модели
            attrs = {
                '__tablename__': table_name,
                '__table__': existing_table
            }
            class_name = table_name.title().replace('_', '')
        
        # ПРАВИЛЬНОЕ создание класса - используем type с правильными параметрами
            model_class = type(
                class_name,
                (self.Base,),
                attrs
            )            
            # Кэшируем модель
            self._models[table_name] = model_class
            logger.info(f"Reflected existing table '{table_name}' into model")
            
            return model_class
            
        except Exception as e:
            logger.error(f"Error reflecting table '{table_name}': {str(e)}")
            raise
    def _reflect_existing_table(self, table_name: str) -> Any:
        """Создает модель из существующей таблицы через automap"""
        try:
            # Создаем automap base
            AutomapBase = automap_base()
            AutomapBase.prepare(self.engine, reflect=True)
            
            # Получаем класс из automap
            if hasattr(AutomapBase.classes, table_name):
                model_class = getattr(AutomapBase.classes, table_name)
            else:
                # Альтернативный способ поиска класса
                for class_name, cls in AutomapBase.classes._items():
                    if hasattr(cls, '__tablename__') and cls.__tablename__ == table_name:
                        model_class = cls
                        break
                else:
                    raise ValueError(f"Table '{table_name}' not found in reflected classes")
            
            # Кэшируем модель
            self._models[table_name] = model_class
            logger.info(f"Reflected existing table '{table_name}' into model using automap")
            
            return model_class
            
        except Exception as e:
            logger.error(f"Error reflecting table '{table_name}': {str(e)}")
            raise
    # CRUD методы становятся ПРОЩЕ
    def create_record(self, table_name: str, data: Dict[str, Any], columns_config: Dict[str, Any] = None) -> Any:
        """
        Создает запись. Если таблицы нет и передан columns_config - создает таблицу.
        """
        session = self.Session()
        try:
            if not self._table_exists(table_name):
                logger.info(f"Указанной таблицы не существует")
                raise ValueError
            model_class = self._get_model(table_name, columns_config)
            instance = model_class(**data)
            session.add(instance)
            session.commit()
            logger.info(f"Created record in '{table_name}' with ID: {instance.id}")
            return instance
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating record in '{table_name}': {str(e)}")
            raise
        finally:
            session.close()

    def read(self, table_name: str, record_id: int) -> Optional[Any]:
        """Читает запись по ID"""
        session = self.Session()
        try:
            if not self._table_exists(table_name):
                logger.info(f"Указанной таблицы не существует")
                raise ValueError
            model_class = self._get_model(table_name)  #  Просто получаем модель
            instance = session.query(model_class).get(record_id)
            if instance:
                return instance
            print(f"В таблице {table_name} не найдено юзера с id{record_id}")
        except Exception as e:
            logger.error(f"Error reading record {record_id} from '{table_name}': {str(e)}")
            raise
        finally:
            session.close()

    def read_all(self, table_name: str, filters: Dict[str, Any] = None) -> List[Any]:
        """Читает все записи"""
        session = self.Session()
        try:
            if not self._table_exists(table_name):
                logger.info(f"Указанной таблицы не существует")
                raise ValueError
            model_class = self._get_model(table_name)  # Просто получаем модель
            query = session.query(model_class)
            
            if filters:
                for field, value in filters.items():
                    if hasattr(model_class, field):
                        query = query.filter(getattr(model_class, field) == value)
            
            results = query.all()
            return results
        except Exception as e:
            logger.error(f"Error reading records from '{table_name}': {str(e)}")
            raise
        finally:
            session.close()

    def update(self, table_name: str, record_id: int, data: Dict[str, Any]) -> Optional[Any]:
        """Обновляет запись"""
        session = self.Session()
        try:
            if not self._table_exists(table_name):
                logger.info(f"Указанной таблицы не существует")
                raise ValueError
            model_class = self._get_model(table_name)  # Просто получаем модель
            instance = session.query(model_class).get(record_id)
            
            if instance:
                for key, value in data.items():
                    if hasattr(instance, key):
                        setattr(instance, key, value)
                session.commit()
                return instance
            logger.error(f"В таблице {table_name} не найден юзер по id{record_id}")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating record {record_id} in '{table_name}': {str(e)}")
            raise
        finally:
            session.close()

    def delete(self, table_name: str, record_id: int) -> bool:
        """Удаляет запись"""
        session = self.Session()
        try:
            if not self._table_exists(table_name):
                raise ValueError(f"указанной таблицы не существует")

            model_class = self._get_model(table_name)  # ✅ Просто получаем модель
            instance = session.query(model_class).get(record_id)
            
            if instance:
                session.delete(instance)
                session.commit()
                return True
            logger.warning(f"Указанного id в таблице {table_name} не сущесвтует")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting record {record_id} from '{table_name}': {str(e)}")
            raise
        finally:
            session.close()

    def delete_table(self, table_name: str) -> bool:
        """Удаляет таблицу - ОЧИЩАЕМ КЭШ"""
        try:
            if self._table_exists(table_name):
                # Удаляем из БД
                self._metadata.reflect(bind=self.engine, only=[table_name])
                if table_name in self._metadata.tables:
                    self._metadata.tables[table_name].drop(self.engine)
                # очищаем кэш
                self._models.pop(table_name, None)
                
                logger.info(f"Table '{table_name}' dropped successfully")
                return True
            logger.error(f"Указанной таблицы нет в БД")
            return False
        except Exception as e:
            logger.error(f"Error dropping table '{table_name}': {str(e)}")
            raise
