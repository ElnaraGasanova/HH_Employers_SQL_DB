from utils.utils import create_table
from utils.utils import fill_in_table
from src.db_manager import DBManager

import os
import psycopg2

PG_KEY: str = os.getenv('POSTGRESSQL_KEY')


def main():
    """Код для проверки работоспособности программы"""
    employers_list = [4649269, 2733062, 15478, 42481, 907345, 4934, 2624085, 1373, 23427, 4023]
    dbmanager = DBManager()
    create_table()
    fill_in_table(employers_list)

    while True:

        task = input(
            "Получить список всех компаний и количество вакансий у каждой компании, введите: 1\n"
            "Получить список всех вакансий с указанием названия компании, вакансии, зарплаты "
            "и ссылки на вакансию, введите: 2\n"
            "Получить среднюю зарплату по вакансиям, введите: 3\n"
            "Получить список всех вакансий, где зарплата выше средней по всем вакансиям, введите: 4\n"
            "Получить список всех вакансий, согласно слова, переданного в метод, введите: 5\n"
            "Завершить работу, введите: Стоп\n"
        )

        if task == "Стоп":
            break
        elif task == '1':
            print(dbmanager.get_companies_and_vacancies_count())
            print()
        elif task == '2':
            print(dbmanager.get_all_vacancies())
            print()
        elif task == '3':
            print(dbmanager.get_avg_salary())
            print()
        elif task == '4':
            print(dbmanager.get_vacancies_with_higher_salary())
            print()
        elif task == '5':
            keyword = input('Введите ключевое слово: ')
            print(dbmanager.get_vacancies_with_keyword(keyword))
            print()
        else:
            print('Неправильный запрос')


main()
