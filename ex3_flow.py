from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import json

#Пути до файлов источников данных и базы данных
work_dir = Path(Path.cwd(), 'work/python_course')
http_egrul = 'https://ofdata.ru/open-data/download/egrul.json.zip'
local_egrul = Path('/media/dmt/win_disk','egrul.json.zip')
airflow_db_connection = 'hw1'
out_file_name = Path(work_dir, 'top10_vacancies.xlsx')
log_name = Path(work_dir, 'tmp.log')

vacancies_table = 'vacancies'
companies_table = 'telecom_companies'
user_agent = {'User-agent': 'Mozilla/5.0'}
url_api = 'https://api.hh.ru/vacancies'
###


default_dag_args = {
    'owner': 'Dmitry Tarasov',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}
#
def parse_egrul_empty():
    pass
#
def parse_egrul_json():
    import zipfile
    import json
#Список полей для таблицы с компаниями
    col_list = ['code', 'ogrn', 'inn', 'name', 'full_name']

    data_list = [] 
    try:
        with zipfile.ZipFile(local_egrul, mode='r') as zf: # Открытие файла с организациями
            for f_name in zf.namelist():
                with zf.open(f_name) as f:      # распаковка очередного файла в памяти
                    json_data = json.load(f)    # десериализация всего файла
                # разбор строки json файла
                for j_str in json_data:         
                    dic_str = {}
                    try:                        # попытка найти основной код ОКВЭД
                        code = j_str['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                        if code.split('.')[0] == '61': # Если основной код = 61 берём
                            dic_str['code'] = code
                        else:                   # иначе пропускаем всю строку
                            continue
                    except:
                        continue                # основной код не найден, пропускаем всю строку
                    for s in col_list[1:]:      # проверяем есть ли нужные поля
                        if s in j_str.keys():
                            dic_str[s] = j_str[s]
                        else:                   #  если нет, заменяем на пусто
                            dic_str[s]=''       #
                    data_list.append(dic_str)   # добавляем строку в список строк

    except zipfile.BadZipFile as error:
        print(error)
            
    df = pd.DataFrame(data_list)
    try:
        connection = SqliteHook(sqlite_conn_id=airflow_db_connection).get_sqlalchemy_engine()
    except:
        pass
    try:
        df.to_sql(companies_table, connection, if_exists='replace', index=False)
    except:
        pass    

#Данные по api hh.ru
def load_api_hh_vacancies():
    from bs4 import BeautifulSoup
    import time
    import requests

    # hh parameters
    params = {'area': '113'
              ,'text': 'middle python developer'
              ,'search_field': 'description'
              ,'industry': '9'
              ,'professional_role': '96'
              ,'items_on_page': '20'
              ,'page': '0'
             }
    links = []
    page = 0
    data_list = []

    #Составляем список 100 ссылок на вакансии
    while (len(data_list) < 100) and (page < 10):
        params['page'] = str(page)
        result = requests.get(url_api, params = params, headers = user_agent)

        if result.status_code != 200:
            page += 1
            continue
            
        result_json = result.json()
        
        links = [l['url'].split('?')[0] for l in result_json['items']]
        page += 1
        
        for url_vacancy in links:
            result2 = requests.get(url_vacancy, headers = user_agent)
            if result2.status_code != 200:
                continue
    
            result2_json = result2.json()
            dic_str = {}
            #Название компании       
            dic_str['company_name'] = result2_json['employer']['name']
            #Название позиции
            dic_str['position'] = result2_json['name']
            #Описание вакансии
            dic_str['job_description'] = BeautifulSoup(result2_json['description'], 'lxml').get_text()
            #Ключевые навыки
            dic_str['key_skills'] = '; '.join([s['name'] for s in result2_json["key_skills"]])
            if 'Python' not in dic_str['key_skills']:  #Не берём, если отсутствует требование по Python             
                continue
            data_list.append(dic_str)
            
            time.sleep(0.5) #Если делать без паузы, то через много страниц может выдать ошибку страницы
    
    df = pd.DataFrame(data_list)
    connection = SqliteHook(sqlite_conn_id=airflow_db_connection).get_sqlalchemy_engine()
    df.to_sql(vacancies_table, connection, if_exists='replace', index=False)

#
def top10_skills():
    connection = SqliteHook(sqlite_conn_id=airflow_db_connection).get_sqlalchemy_engine()
    df_vacancies = pd.read_sql(f'select * from {vacancies_table}', connection)

    #Вывод топ 10 вакансий в эксель файл
    (df_vacancies
      .assign(key_skills = lambda x: x['key_skills'].str.split('; '))
      .explode('key_skills')
      .value_counts("key_skills")
      .rename('num_vacancies')[:10]
      .to_excel(out_file_name)
    )

###DAG construction
with DAG(
    dag_id='Exercise_3',
    default_args=default_dag_args,
    description='Using Apache Airflow for getting hh.ru vacancies and so on',
    schedule=None,
    start_date=datetime(2023, 7, 27, 12),
    #schedule_interval='@daily'
) as dag:
    # Загрузка вакансий с hh.ru по компаниям из телекома с поиском сотрудников с навыком middle+python
    hh_vacancies_task = PythonOperator(
        task_id = 'load_hh_vacancies',
        python_callable = load_api_hh_vacancies,
    )
    # Загрузка файла со справочником ЕГРЮЛ
    download_egrul_task = BashOperator(
        task_id = 'download_egrul',
        #bash_command = f'wget {http_egrul} -O {local_egrul}', 
        bash_command = '', 
    )
    # Парсинг из архива организаций с кодом 61
    parse_egrul_task = PythonOperator(
        task_id = 'parse_egrul',
        #python_callable = parse_egrul_json,
        python_callable = parse_egrul_empty,
    )  
    # Составление топ 10 требований для вакансий с middle+python
    top10_task = PythonOperator(
        task_id = 'top10_vacancies',
        python_callable = top10_skills,
    )
    
    hh_vacancies_task >> top10_task
    download_egrul_task >> parse_egrul_task >> top10_task
###    
    
