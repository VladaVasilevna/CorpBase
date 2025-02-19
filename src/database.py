from typing import Dict, List, Optional

from dotenv import load_dotenv

from src.get_vacancies import fetch_vacancies_for_specific_employers, fetch_employer_name
from src.db_connection import DBConnection
from src.db_queries import DBQueries


def create_database(queries_manager: DBQueries) -> None:
    """Создает базу данных, если она не существует."""
    load_dotenv()

    db_name = "companies_and_vacancies"

    # Проверяем, существует ли база данных, используя запрос
    check_db_query = f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"

    # Если база данных не существует, создаем ее
    if not queries_manager.execute_query(check_db_query):
        create_db_query = f"CREATE DATABASE {db_name}"
        queries_manager.execute_query(create_db_query)


def setup_employers_table(queries_manager: DBQueries) -> str:
    """Создает или пересоздает таблицу employers в базе данных."""
    try:
        queries_manager.execute_query("DROP TABLE IF EXISTS employers CASCADE;")  # Удаляем таблицу, если она существует
        queries_manager.execute_query(
            "CREATE TABLE employers (employer varchar(100) PRIMARY KEY not null, open_vacancies varchar(100))"
        )
        return "Таблица 'employers' успешно создана/пересоздана."
    except Exception as e:
        print(e)
        return "Ошибка при создании таблицы employers."


def populate_employers_table(employers_list: List[Dict[str, Optional[str]]], queries_manager: DBQueries) -> str:
    """Заполняет таблицу employers данными."""
    try:
        for employer in employers_list:
            name_employer = employer["name"]
            open_vacancies = employer["open_vacancies"]
            queries_manager.execute_query(
                "INSERT INTO employers VALUES (%s, %s) ON CONFLICT (employer) DO NOTHING",  # Избегаем дубликатов
                (name_employer, open_vacancies),
            )
        return "Работодатели успешно добавлены в таблицу 'employers'."
    except Exception as e:
        print(e)
        return "Ошибка при заполнении таблицы employers."


def setup_vacancies_table(queries_manager: DBQueries) -> str:
    """Создает или пересоздает таблицу vacancies в базе данных."""
    try:
        queries_manager.execute_query("DROP TABLE IF EXISTS vacancies;")  # Удаляем таблицу, если она существует
        queries_manager.execute_query(
            """
            CREATE TABLE vacancies (
                name_vacancy text not null,
                employer varchar(100) not null,
                location varchar(100) not null,
                salary_from INT not null,
                salary_to INT not null,
                currency varchar(10) not null,
                url text not null,
                FOREIGN KEY (employer) REFERENCES employers(employer),
                UNIQUE (name_vacancy, employer)  -- Добавляем уникальный индекс
            )
            """
        )
        return "Таблица 'vacancies' успешно создана/пересоздана."
    except Exception as e:
        print(e)
        return "Ошибка при создании таблицы vacancies."


def populate_vacancies_table(vacancies_list: List[Dict[str, dict]], queries_manager: DBQueries) -> str | None:
    """Заполняет таблицу vacancies данными."""
    try:
        for vacancy in vacancies_list:
            name_vacancy = vacancy["name"]
            employer = vacancy["employer"].get("name")
            location = vacancy["area"]["name"]
            salary_from = (
                vacancy["salary"]["from"] if vacancy["salary"]["from"] is not None else 0
            )
            salary_to = vacancy["salary"]["to"] if vacancy["salary"]["to"] is not None else 0
            currency = vacancy["salary"]["currency"]
            url_vacancy = vacancy["alternate_url"]
            queries_manager.execute_query(
                "INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name_vacancy, employer) DO NOTHING",  # Избегаем дубликатов
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
        return "Вакансии успешно добавлены в таблицу 'vacancies'."
    except Exception as e:
        print(e)
        return "Ошибка при заполнении таблицы vacancies."


if __name__ == "__main__":
    load_dotenv()
    db_connection = DBConnection()

    local_queries_manager = DBQueries(db_connection)

    create_database(local_queries_manager)

    print(setup_employers_table(local_queries_manager))
    # Список ID конкретных работодателей
    specific_employer_ids = ["80", "1740", "2460946", "15478", "4233", "59", "1102601", "208707", "1373", "106571"]
    #  Получаем данные о работодателях по их ID
    employers_data = []
    for employer_id in specific_employer_ids:
        employer_name = fetch_employer_name(employer_id)
        if employer_name:
            employers_data.append({"name": employer_name, "open_vacancies": "N/A"})  #  "N/A" или другое значение
        else:
            print(f"Не удалось получить имя для работодателя с ID {employer_id}")
    print(populate_employers_table(employers_data, local_queries_manager))

    print(setup_vacancies_table(local_queries_manager))
    vacancies_data = fetch_vacancies_for_specific_employers(specific_employer_ids)
    print(populate_vacancies_table(vacancies_data, local_queries_manager))
