import psycopg2
import os

PG_KEY=os.getenv('POSTGRESSQL_KEY')
password = PG_KEY



class DBManager:
    """Класс подключения и работы к БД PostgreSQL"""

    @staticmethod
    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний
        и количество вакансий у каждой компании."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name, COUNT(vacancies_name) AS count_vacancies  "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id) "
                            f"GROUP BY employers.company_name")
                result = cur.fetchall()
            conn.commit()
        return result

    @staticmethod
    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием
        названия компании, названия вакансии и зарплаты
        и ссылки на вакансию."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employers.company_name, vacancies.vacancies_name, "
                            f"vacancies.payment, vacancies_url "
                            f"FROM employers "
                            f"JOIN vacancies USING (employer_id)")
                result = cur.fetchall()
            conn.commit()
        return result

    @staticmethod
    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT AVG(payment) as avg_payment FROM vacancies ")
                result = cur.fetchall()
            conn.commit()
        return result

    @staticmethod
    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых
        зарплата выше средней по всем вакансиям."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE payment > (SELECT AVG(payment) FROM vacancies) ")
                result = cur.fetchall()
            conn.commit()
        return result

    @staticmethod
    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых
        содержатся переданные в метод слова, например python."""
        with psycopg2.connect(host="localhost", database="hh_employers_db",
                              user="postgres", password=PG_KEY) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE lower(vacancies_name) LIKE '%{keyword}%' "
                            f"OR lower(vacancies_name) LIKE '%{keyword}'"
                            f"OR lower(vacancies_name) LIKE '{keyword}%';")
                result = cur.fetchall()
            conn.commit()
        return result


def format_salary(salary_from, salary_to):
    if salary_from is None:
        salary_from = 'не указано'
    if salary_to is None:
        salary_to = 'не указано'
    return salary_from, salary_to
