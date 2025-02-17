from src.get_vacancies import get_employers, get_vacancies
from src.vacancies_to_table import (
    create_database,
    employers_to_table,
    vacancies_to_table,
)
from src.working_with_vacancies import DBManager

if __name__ == "__main__":
    print(get_vacancies(get_employers()))
    create_database()
    print(employers_to_table(get_employers()))
    print(vacancies_to_table(get_vacancies(get_employers())))
    obj = DBManager("python")
    print(obj.get_companies_and_vacancies_count())
    print(obj.get_all_vacancies())
    print(obj.get_avg_salary())
    print(obj.get_vacancies_with_higher_salary())
    print(obj.get_vacancies_with_keyword())
