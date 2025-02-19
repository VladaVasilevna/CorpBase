import os

import psycopg2


class DBConnection:
    """Управляет подключением к базе данных PostgreSQL."""

    def __init__(self):
        """Инициализирует параметры подключения к базе данных из переменных окружения."""
        self.host = os.environ.get("DB_HOST", "localhost")
        self.port = os.environ.get("DB_PORT", "5432")
        self.user = os.environ.get("DB_USER", "postgres")
        self.password = os.environ.get("DATABASE_PASSWORD")
        self.database = os.environ.get("DB_NAME", "companies_and_vacancies")

        if not self.password:
            raise ValueError("Необходимо установить переменную окружения DB_PASSWORD")

    def get_connection(self):
        """Устанавливает и возвращает подключение к базе данных."""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            return conn
        except psycopg2.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            return None
