
def _create_db_url(db_brand:str,db_lib:str,db_config:dict[str:str]):
    '''метод для генерации ссылки к бд при подключении через sqlalchemy'''
    if isinstance(db_brand,str) and isinstance(db_lib,str) and isinstance(db_config,dict):
        return f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    print(f"введены неверные параметры для настройки db_url:{db_brand},{db_lib},{db_config}")
    return False