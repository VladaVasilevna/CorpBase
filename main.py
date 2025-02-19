from dotenv import load_dotenv

from src.get_vacancies import fetch_vacancies_for_specific_employers, fetch_employer_name
from src.database import (
    create_database,
    setup_employers_table,
    populate_employers_table,
    setup_vacancies_table,
    populate_vacancies_table,
)
from src.companies_and_vacancies import DBManager
from src.db_connection import DBConnection
from src.db_queries import DBQueries


def main():
    """Основная функция для выполнения рабочего процесса программы."""
    load_dotenv()
    #  Подключение к базе данных
    db_connection = DBConnection()
    db_queries = DBQueries(db_connection)

    # Создание базы данных (если она не существует)
    create_database(db_queries)

    # Настройка и заполнение таблицы работодателей
    print(setup_employers_table(db_queries))

    # Список ID конкретных работодателей
    specific_employer_ids = ["80", "1740", "2460946", "15478", "4233", "59", "1102601", "208707", "1373", "106571"]

    # Получаем данные о работодателях по их ID
    employers_data = []
    for employer_id in specific_employer_ids:
        employer_name = fetch_employer_name(employer_id)
        if employer_name:
            employers_data.append({"name": employer_name, "open_vacancies": "N/A"})
        else:
            print(f"Не удалось получить имя для работодателя с ID {employer_id}")

    print(populate_employers_table(employers_data, db_queries))

    # Настройка и заполнение таблицы вакансий
    print(setup_vacancies_table(db_queries))

    vacancies_data = fetch_vacancies_for_specific_employers(specific_employer_ids)

    print(populate_vacancies_table(vacancies_data, db_queries))

    obj = DBManager("Менеджер", db_queries)

    print("\nКомпании и количество вакансий:", obj.get_companies_and_vacancies_count())
    print("Все вакансии:", obj.get_all_vacancies())
    print("Средняя зарплата:", obj.get_avg_salary())
    print("Вакансии с зарплатой выше средней:", obj.get_vacancies_with_higher_salary())
    print("Вакансии с ключевым словом 'Менеджер':", obj.get_vacancies_with_keyword())


if __name__ == "__main__":
    main()
