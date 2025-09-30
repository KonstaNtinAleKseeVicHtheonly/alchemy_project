from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Boolean, MetaData # параметр столбцов при создании модели
from configuration.db_config import DB_CONFIG
from datetime import datetime
import logging
from configuration.db_url_config import _create_db_url # для генерации db_url при создании эк менеджера
# интерфейсы 
from interface.db_manager_interface import DBManagerInterface

# Логгирование
logging.basicConfig(
    level=logging.DEBUG,#задаем уровень отображения логгеров(здесь от дебага и выше)
    format='[%(asctime)s] #%(levelname)-8s %(filename)s:'
           '%(lineno)d - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

db_url = _create_db_url('postgresql','psycopg2',DB_CONFIG)

def main_alternative():
    db_url = _create_db_url('postgresql','psycopg2',DB_CONFIG) # генерация db_url
    interface_manager = DBManagerInterface(db_url)

    # создание таблицы
    # my_table_name = "users228"
    # users_columns = {
    # 'username': String(50),
    # 'email': String(100),
    # 'age': Integer,
    # 'is_active': Boolean,
    # 'created_at': DateTime
    # }
    # # Создаем модель на лету
    # alternative_manager.create_model(my_table_name, users_columns)
    # #CRUD операции
    # #создание строки
    user1 = interface_manager.create_record('users228', {
        'username': 'пупа и лупа',
        'email': 'alex@example.com',
        'age': 14,
        'is_active': True,
        'created_at': datetime.now()
    })
    return user1
    # return user1
    # # чтение строки
    # return inferface_manager.read_all('users228')
    # # чтение всех строк
    # alternative_manager.read_all('users228')
    # обновление инфы в строке  
    # updated_user = interface_manager.update('users228', 4, {
    # 'age': 14,
    # 'email': 'mardssdaasadsia_new@example.com'
    # })
    # return updated_user
    # удаление записи 
    deleted = interface_manager.delete('users', 4)
    # удаление таблицы
    return deleted



if __name__ == "__main__":
    # Настройки подключения к PostgreSQL
    print(main_alternative())