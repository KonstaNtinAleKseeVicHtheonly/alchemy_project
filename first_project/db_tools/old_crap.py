class DynamicModelManager:
    '''класс для содержащий методы для взаимодействия с таблицами(создание, изменение, удаление)'''
    def __init__(self, db_url: str):
        """
        Инициализация менеджера динамических моделей
        
        Args:
            db_url: URL подключения к PostgreSQL
                   format: postgresql+psycopg2://user:password@host:port/database
        """
        self.db_url = db_url # ссылка ждя подключения к бд
        self.engine = create_engine(db_url, echo=False)
        self.Base = declarative_base()
        self.Session = sessionmaker(bind=self.engine)
        self.created_models = {}
        
        logger.info(f"Initialized DynamicModelManager for database: {db_url}")

    def create_dynamic_model(self, table_name: str, columns_config: dict):
        """
        Динамически создает модель и таблицу в БД
        
        Args:
            table_name: Имя таблицы
            columns_config: Словарь с конфигурацией колонок
                           format: {'column_name': ('type', {options})}
                           supported types: integer, string, text, datetime, float, boolean
        """
        try:
            # глобальная проверка таблицы в бд
            if self._table_exists(table_name):
                logger.warning(f"Table {table_name} already exists in database")
                # Можно либо вернуть существующую модель, либо бросить исключение
                # return self._get_existing_model(table_name)
                raise ValueError(f"Table {table_name} already exists")
            # локальная проверка в рамках ЭК
            if table_name in self.created_models:
                logger.warning(f"Таблица {table_name} уже существуе")
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
            logger.info(f"Создана таблица '{table_name}' с колонками: {list(columns_config.keys())}")
            
            return model_class
        except Exception as e:
            logger.error(f"Произошла ошибка при создании модели {table_name}: {str(e)}")
            raise
    
    def _table_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица в базе данных"""
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)
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
            
            # ЕСЛИ МОДЕЛЬ ЕЩЕ НЕ СОЗДАНА В ЭТОМ ЭКЗЕМПЛЯРЕ - СОЗДАЕМ ЕЕ
            if table_name not in self.created_models:
                logger.warning(f"Model for table {table_name} not found in cache. Creating model from existing table.")
                
            model_class = self.created_models[table_name]
            session = self.get_session()
        
            instance = model_class(**data)
            session.add(instance)
            session.commit()
            model_class
            logger.info(f"Successfully inserted data into {table_name}")
            return instance
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении данных в  {table_name}: {str(e)}")
            raise

    def get_all_data(self, table_name: str):
        """Получает все данные из таблицы"""
        try:
            if not self._table_exists(table_name):
                logger.warning(f"Нельзя получить данные из несуществующей таблиц {table_name} already exists in database")
                # Можно либо вернуть существующую модель, либо бросить исключение
                # return self._get_existing_model(table_name)
                raise ValueError(f"Table {table_name} already exists")
            if table_name not in self.created_models:
                raise ValueError(f"Table {table_name} not found")

            model_class = self.created_models[table_name]
            session = self.get_session()
            
            return session.query(model_class).all()
        except Exception as e:
            logger.error(f"Ошибка получения данных из таблицы{table_name}: {str(e)}")
            raise

    def drop_table(self, table_name: str):
        """Удаляет таблицу из БД"""
        try:
            if not self._table_exists(table_name):
                logger.warning(f"Нельзя удалить несуществующую таблицу {table_name} already exists in database")
                raise ValueError(f"Table {table_name} already exists")
            if table_name in self.created_models:
                model_class = self.created_models[table_name]
                model_class.__table__.drop(self.engine)
                del self.created_models[table_name]
                logger.info(f"Table {table_name} dropped successfully")
        except Exception as e:
            logger.error(f"Ошибка при удалении таблицы {table_name}: {str(e)}")
            raise


# Пример использования
if __name__ == "__main__":
    # Настройки подключения к PostgreSQL
    DB_CONFIG = {
        'user': os.getenv('DATABASE_USER'),
        'password': os.getenv('DATABASE_PASSWORD'),
        'host': os.getenv('DATABASE_HOST'),
        'port': os.getenv('DATABASE_PORT'),
        'database': os.getenv('DATABASE_NAME')
    }
    
    # Формируем URL для подключения
    DATABASE_URL = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    # Создаем менеджер
    model_manager = DynamicModelManager(DATABASE_URL)
    
    try:
        # Пример 1: Создаем таблицу пользователей
        users_columns = {
            'username': ('string', {'length': 100, 'nullable': False, 'unique': True}),
            'email': ('string', {'length': 255, 'nullable': False}),
            'age': ('integer', {'nullable': True}),
            'created_at': ('datetime', {'default': datetime.now}),
            'is_active': ('boolean', {'default': True})
        }
        # создание модели
        UserModel = model_manager.create_dynamic_model('users', users_columns)
        
        # Добавляем тестовые данные
        test_user = {
            'username': 'john_doe',
            'email': 'john@example.com',
            'age': 30,
            'is_active': True
        }
        
    #     model_manager.insert_data('users', test_user)
        
    #     # Пример 2: Создаем таблицу продуктов
        # products_columns = {
        #     'name': ('string', {'length': 200, 'nullable': False}),
        #     'description': ('text', {'nullable': True}),
        #     'price': ('float', {'nullable': False}),
        #     'in_stock': ('boolean', {'default': True}),
        #     'created_at': ('datetime', {'default': datetime.now})
        # }
        
        # ProductModel = model_manager.create_dynamic_model('products', products_columns)
        
        # # Добавляем тестовый продукт
        # test_product = {
        #     'name': 'Laptop',
        #     'description': 'High-performance laptop',
        #     'price': 999.99,
        #     'in_stock': True
        # }
        
        # model_manager.insert_data('products', test_product)
        
    #     # Получаем все пользователи
    #     users = model_manager.get_all_data('users')
    #     print(f"Found {len(users)} users")
        
    #     # Получаем все продукты
    #     products = model_manager.get_all_data('products')
    #     print(f"Found {len(products)} products")
        
    # except Exception as e:
    #     logger.error(f"Application error: {str(e)}")
    
    finally:
        # Закрываем соединения
        if hasattr(model_manager, 'engine'):
            model_manager.engine.dispose()