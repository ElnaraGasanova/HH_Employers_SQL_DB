import json
import os
import requests
import psycopg2


employer_dict = {}
employers_data = []
vacancies_emp = []


def get_employer(employer):
    """Получение данных c HeadHunter.ru о работодателе."""
    url = 'https://api.hh.ru/employers'
    PG_KEY: str = os.getenv('POSTGRESSQL_KEY')
    params = {
        "per_page": 10,
        "text": {employer},  # поиск по названию работодателя.
        "area": 1,  # Код региона (1 - Москва)
    }
    response = requests.get(url, params=params)
    employer = response.json()
    if employer is None:
        return "Данные не получены."
    elif 'items' not in employer:
        return "Указанный работодатель не найден."
    else:
        employer_dict = {'id': employer['items'][0]['id'], 'name': employer['items'][0]['name'],
                         'alternate_url': employer['items'][0]['alternate_url']}
        employers_data.append(employer_dict)
        return employer_dict


def get_all_vacancies(employer_id, page):
    """Получение данных c HeadHunter.ru о вакансиях работодателя."""
    employer_id = employer_id
    params = {
        'employer_id': employer_id,
        'area': 1,
        'per_page': 10,
        'page': page
    }
    response = requests.get('https://api.hh.ru/vacancies', params)
    data = response.content.decode()
    response.close()
    return data


def get_vacancies(employer_id):
    """Обработка полученной информации по вакансиям,
    количество (range) можно задать любое."""
    vacancies_emp_dicts = []
    for page in range(10):
        vacancies_data = json.loads(get_all_vacancies(employer_id, page))
        if 'errors' in vacancies_data:
            return vacancies_data['errors'][0]['value']
        for vacancy_data in vacancies_data['items']:
            if vacancy_data['salary'] is None:
                vacancy_data['salary'] = {}
                vacancy_data['salary']['from'] = None
                vacancy_data['salary']['to'] = None

            vacancy_dict = {'id': vacancy_data['id'], 'vacancy': vacancy_data['name'],
                            'url': vacancy_data['apply_alternate_url'],
                            'salary_from': vacancy_data['salary']['from'],
                            'salary_to': vacancy_data['salary']['to'],
                            'employer_id': vacancy_data['employer']['id']}
            if vacancy_dict['salary_to'] is None:
                vacancy_dict['salary_to'] = vacancy_dict['salary_from']
            vacancies_emp_dicts.append(vacancy_dict)
    return vacancies_emp_dicts


# данные для проверки работы функций.
print(get_employer('Аэрофлот'))
print(get_vacancies('1373'))


def create_table():
    """SQL. Создание БД и таблиц."""
    conn = psycopg2.connect(host="localhost", database="postgres",
                            user="postgres", password="PG_KEY")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS hh_employers_db")
    cur.execute("CREATE DATABASE hh_employers_db")

    cur.close()
    conn.close()

    conn = psycopg2.connect(host="localhost", database="hh_employers_db",
                            user="postgres", password="PG_KEY")
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE employers
                      (employer_id int PRIMARY KEY,
                       employer_name varchar(50),
                       employer_url varchar(200))""")

        cur.execute(f"""CREATE TABLE vacancies
                      (vacancy_id int PRIMARY KEY,
                       vacancy_name varchar(50),
                       vacancy_url varchar(200),
                       vacancy_salary_from int,
                       vacancy_salary_to int,
                       employer_id int
                       REFERENCES employers(employer_id));""")

    cur.close()
    conn.commit()
    conn.close()


def fill_in_table(employers_list):
    """SQL. Заполнение таблиц данными."""
    with psycopg2.connect(host="localhost", database="hh_employers_db",
                          user="postgres", password="PG_KEY", encoding='utf-8') as conn:
        with conn.cursor() as cur:
            cur.execute('TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')

            for employer in employers_list:
                employer_list = get_employer(employer)
                cur.execute('INSERT INTO employers (employer_id, company_name, open_vacancies) '
                            'VALUES (%s, %s, %s) RETURNING employer_id',
                            (employer_list['employer_id'], employer_list['company_name'],
                             employer_list['open_vacancies']))

            for employer in employers_list:
                vacancy_list = get_vacancies(employer)
                for v in vacancy_list:
                    cur.execute('INSERT INTO vacancies (vacancy_id, vacancies_name, '
                                'payment, requirement, vacancies_url, employer_id) '
                                'VALUES (%s, %s, %s, %s, %s, %s)',
                                (v['vacancy_id'], v['vacancies_name'], v['payment'],
                                 v['requirement'], v['vacancies_url'], v['employer_id']))

        conn.commit()

