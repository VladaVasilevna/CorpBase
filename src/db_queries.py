import psycopg2


class DBQueries:
    """Выполняет SQL-запросы к базе данных PostgreSQL."""

    def __init__(self, db_connection):
        """Инициализирует DBQueries с подключением к базе данных."""
        self.db_connection = db_connection

    def execute_query(
        self, query: str, params: tuple | None = None, is_select: bool = True
    ) -> list:
        """Выполняет SQL-запрос и возвращает результаты."""
        conn = self.db_connection.get_connection()  # Получаем подключение к базе данных
        if not conn:
            return []

        cursor = None  # Инициализируем курсор
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)  # Выполняем SQL-запрос
            if is_select:  # Если запрос - SELECT, получаем результаты
                return cursor.fetchall()
            else:
                conn.commit()  # Фиксируем изменения для не-SELECT запросов
                return []  # Возвращаем пустой список для таких запросов
        except psycopg2.Error as e:
            if conn:
                conn.rollback()  # Откатываем транзакцию в случае ошибки
            return []
        finally:
            if cursor:
                cursor.close()  # Закрываем курсор
