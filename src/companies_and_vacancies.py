from abc import ABC, abstractmethod


class DataFetcher(ABC):
    """Абстрактный базовый класс для извлечения данных."""

    @abstractmethod
    def get_companies_and_vacancies_count(self):
        """Абстрактный метод для получения количества компаний и их вакансий."""
        pass

    @abstractmethod
    def get_all_vacancies(self):
        """Абстрактный метод для получения всех вакансий."""
        pass

    @abstractmethod
    def get_avg_salary(self):
        """Абстрактный метод для получения средней зарплаты."""
        pass

    @abstractmethod
    def get_vacancies_with_higher_salary(self):
        """Абстрактный метод для получения вакансий с зарплатой выше средней."""
        pass

    @abstractmethod
    def get_vacancies_with_keyword(self):
        """Абстрактный метод для получения вакансий с определенным ключевым словом."""
        pass


class DBManager(DataFetcher):
    """Управляет взаимодействием с базой данных для получения информации о компаниях и вакансиях."""

    def __init__(self, keyword: str, queries_manager):
        """Инициализирует DBManager с ключевым словом и экземпляром DBQueries."""
        self.keyword = keyword
        self.queries_manager = queries_manager

    def get_companies_and_vacancies_count(self) -> list:
        """Получает список компаний и количество вакансий у каждой компании."""
        query = """
                SELECT v.employer, COUNT(v.name_vacancy)
                FROM vacancies v
                GROUP BY v.employer
                """
        results = self.queries_manager.execute_query(query)
        return results

    def get_all_vacancies(self) -> list[dict[str, str]]:
        """Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию."""
        query = """
            SELECT v.name_vacancy, v.employer, v.salary_from, v.salary_to, v.currency, v.url
            FROM vacancies v
        """
        results = self.queries_manager.execute_query(query)
        vacancies = []
        for row in results:
            vacancies.append(
                {
                    "Компания": row[1],
                    "Вакансия": row[0],
                    "Зарплата": f"{row[2]} - {row[3]} {row[4]}",
                    "URL": row[5],
                }
            )
        return vacancies

    def get_avg_salary(self) -> str:
        """Получает среднюю зарплату по вакансиям."""
        query = """
            SELECT ROUND(AVG((salary_from + salary_to) / 2), 2) AS avg_salary
            FROM vacancies
            WHERE salary_to > 0 AND salary_from > 0
        """
        results = self.queries_manager.execute_query(query)
        avg_salary = results[0][0] if results and results[0] else None
        return f"Средняя зарплата: {float(avg_salary) if avg_salary else 'N/A'}"

    def get_vacancies_with_higher_salary(self) -> list[dict[str, str]]:
        """Получает список вакансий, у которых зарплата выше средней по всем вакансиям."""
        query = """
            SELECT v.name_vacancy, v.employer, v.salary_from, v.salary_to, v.currency, v.url
            FROM vacancies v
            WHERE (salary_from + salary_to) / 2 > (
                SELECT AVG((salary_from + salary_to) / 2) FROM vacancies
            )
        """
        results = self.queries_manager.execute_query(query)
        vacancies = []
        for row in results:
            vacancies.append(
                {
                    "Компания": row[1],
                    "Вакансия": row[0],
                    "Зарплата": f"{row[2]} - {row[3]} {row[4]}",
                    "URL": row[5],
                }
            )
        return vacancies

    def get_vacancies_with_keyword(self) -> list[dict[str, str]]:
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова."""
        query = f"""
            SELECT v.name_vacancy, v.employer, v.salary_from, v.salary_to, v.currency, v.url
            FROM vacancies v
            WHERE name_vacancy ILIKE '%{self.keyword}%'
        """
        results = self.queries_manager.execute_query(query)
        vacancies = []
        for row in results:
            vacancies.append(
                {
                    "Компания": row[1],
                    "Вакансия": row[0],
                    "Зарплата": f"{row[2]} - {row[3]} {row[4]}",
                    "URL": row[5],
                }
            )
        return vacancies


if __name__ == "__main__":
    from src.db_connection import DBConnection
    from src.db_queries import DBQueries

    db_connection = DBConnection()
    local_queries_manager = DBQueries(db_connection)

    obj = DBManager("Менеджер", local_queries_manager)

    print("Компании и количество вакансий:", obj.get_companies_and_vacancies_count())
    print("Все вакансии:", obj.get_all_vacancies())
    print("Средняя зарплата:", obj.get_avg_salary())
    print("Вакансии с зарплатой выше средней:", obj.get_vacancies_with_higher_salary())
    print("Вакансии с ключевым словом 'Менеджер':", obj.get_vacancies_with_keyword())
