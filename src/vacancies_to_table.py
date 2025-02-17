import os
from typing import Dict, List, Optional

import psycopg2
from dotenv import load_dotenv

from src.get_vacancies import get_employers, get_vacancies


def create_database() -> None:
    """Creates the database if it doesn't exist."""
    load_dotenv()

    host = "localhost"
    port = "5432"
    user = "postgres"
    password = "1234"
    database = "companies_and_vacancies"

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, database=database
    )
    conn.autocommit = True

    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'companies_and_vacancies';")
    exists = cur.fetchone()
    try:
        if not exists:
            cur.execute("CREATE DATABASE companies_and_vacancies")
            print("База данных vacancies успешно создана.")
    except psycopg2.DatabaseError as e:
        print("Не удалось создать базу данных", e)
    finally:
        cur.close()
        conn.close()


def employers_to_table(employers: List[dict[str, Optional[str]]]) -> str:
    """Function to add employers to the table"""
    create_database()
    load_dotenv()
    conn_params = {
        "host": "localhost",
        "database": "companies_and_vacancies",
        "user": "postgres",
        "password": "1234",
    }

    with psycopg2.connect(**conn_params) as conn:  # type: ignore
        with conn.cursor() as cur:
            try:
                cur.execute("DROP TABLE IF EXISTS employers CASCADE;")
                cur.execute(
                    "CREATE TABLE employers (employer varchar(100) PRIMARY KEY not null,"
                    "open_vacancies varchar(100))"
                )

                for employer in employers:
                    name_employer = employer["name"]
                    open_vacancies = employer["open_vacancies"]
                    cur.execute(
                        "INSERT INTO employers VALUES (%s, %s)",
                        (name_employer, open_vacancies),
                    )

                return "Работодатели добавлены в таблицу"
            except Exception as e:
                print(e)
                return "Ошибка в employers_to_table"


def vacancies_to_table(vacancies: List[Dict[str, dict]]) -> str | None:
    """Function to add vacancies to the table"""
    create_database()
    load_dotenv()
    conn_params = {
        "host": "localhost",
        "database": "companies_and_vacancies",
        "user": "postgres",
        "password": "1234",
    }

    with psycopg2.connect(**conn_params) as conn:  # type: ignore
        with conn.cursor() as cur:
            try:
                cur.execute("DROP TABLE IF EXISTS vacancies;")
                cur.execute(
                    "CREATE TABLE vacancies (name_vacancy text not null, "
                    "employer varchar(100) not null, "
                    "location varchar(100) not null, "
                    "salary_from INT not null, salary_to INT not null, "
                    "currency varchar(10) not null, url text not null,"
                    "FOREIGN KEY (employer) REFERENCES employers(employer));"
                )
                for vacancy in vacancies:
                    name_vacancy = vacancy["name"]
                    employer = vacancy["employer"].get("name")
                    location = vacancy["area"]["name"]
                    salary_from = (
                        vacancy["salary"]["from"]
                        if vacancy["salary"]["from"] is not None
                        else 0
                    )
                    salary_to = (
                        vacancy["salary"]["to"]
                        if vacancy["salary"]["to"] is not None
                        else 0
                    )
                    currency = vacancy["salary"]["currency"]
                    url_vacancy = vacancy["alternate_url"]
                    cur.execute(
                        "INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (
                            name_vacancy,
                            employer,
                            location,
                            salary_from,
                            salary_to,
                            currency,
                            url_vacancy,
                        ),
                    )

                return "Вакансии добавлены в таблицу"
            except Exception as e:
                print(e)
                return "Ошибка в get_vacancies"


if __name__ == "__main__":
    print(employers_to_table(get_employers()))
    print(vacancies_to_table(get_vacancies(get_employers())))
