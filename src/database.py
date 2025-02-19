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
        queries_manager.execute_query("DROP TABLE IF EXISTS employers CASCADE;", is_select=False)
        queries_manager.execute_query(
            """
            CREATE TABLE IF NOT EXISTS employers (
                employer varchar(100) PRIMARY KEY not null,
                open_vacancies varchar(100)
            )
            """,
            is_select=False,
        )
        return "Таблица 'employers' успешно создана/пересоздана."
    except Exception as e:
        print(f"Ошибка при создании таблицы employers: {e}")
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
        queries_manager.execute_query("DROP TABLE IF EXISTS vacancies;", is_select=False)
        queries_manager.execute_query(
            """
            CREATE TABLE IF NOT EXISTS vacancies (
                name_vacancy text not null,
                employer varchar(100) not null,
                location varchar(100) not null,
                salary_from INT not null,
                salary_to INT not null,
                currency varchar(10) not null,
                url text not null,
                FOREIGN KEY (employer) REFERENCES employers(employer),
                UNIQUE (name_vacancy, employer)
            )
            """,
            is_select=False,
        )
        return "Таблица 'vacancies' успешно создана/пересоздана."
    except Exception as e:
        print(f"Ошибка при создании таблицы vacancies: {e}")
        return "Ошибка при создании таблицы vacancies."


def populate_vacancies_table(vacancies_list: List[Dict[str, dict]], queries_manager: DBQueries) -> str | None:
    """Заполняет таблицу vacancies данными."""
    try:
        for vacancy in vacancies_list:
            name_vacancy = vacancy["name"]
            employer_name = vacancy["employer"].get("name")
            employer_id = vacancy["employer"].get("id")  # Получаем ID работодателя
            location = vacancy["area"]["name"]
            salary_from = (
                vacancy["salary"]["from"] if vacancy["salary"]["from"] is not None else 0
            )
            salary_to = vacancy["salary"]["to"] if vacancy["salary"]["to"] is not None else 0
            currency = vacancy["salary"]["currency"]
            url_vacancy = vacancy["alternate_url"]

            # Проверяем, существует ли работодатель в таблице 'employers'
            check_employer_query = "SELECT employer FROM employers WHERE employer = %s"
            employer_exists = queries_manager.execute_query(check_employer_query, (employer_name,))

            if not employer_exists:
                # Если работодателя нет, добавляем его
                insert_employer_query = """
                    INSERT INTO employers (employer, open_vacancies)
                    VALUES (%s, %s)
                    ON CONFLICT (employer) DO NOTHING
                """
                queries_manager.execute_query(insert_employer_query, (employer_name, "N/A"), is_select=False)

            # Вставляем вакансию
            queries_manager.execute_query(
                """
                INSERT INTO vacancies (name_vacancy, employer, location, salary_from, salary_to, currency, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name_vacancy, employer) DO NOTHING
                """,
                (
                    name_vacancy,
                    employer_name,
                    location,
                    salary_from,
                    salary_to,
                    currency,
                    url_vacancy,
                ),
                 is_select=False # Указываем, что это INSERT запрос
            )

        return "Вакансии успешно добавлены в таблицу 'vacancies'."
    except Exception as e:
        print(f"Ошибка при заполнении таблицы vacancies: {e}")
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
