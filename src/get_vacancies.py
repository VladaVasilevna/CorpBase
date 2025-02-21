from typing import Any, Dict, List

import requests


def fetch_vacancies_for_specific_employers(
    employer_ids: List[str],
) -> List[Dict[str, Any]]:
    """Получает список вакансий для указанных ID работодателей."""
    all_vacancies = []

    for employer_id in employer_ids:
        employer_vacancies = fetch_vacancies_by_employer_id(employer_id)
        if employer_vacancies:
            all_vacancies.extend(employer_vacancies)

    return all_vacancies


def fetch_vacancies_by_employer_id(employer_id: str) -> List[Dict[str, Any]]:
    """Получает список вакансий для одного работодателя по его ID."""
    url = "https://api.hh.ru/vacancies"
    params = {"employer_id": employer_id}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Проверка на HTTP ошибки

        vacancies_data = response.json()
        employer_vacancies = vacancies_data.get("items", [])

        valid_vacancies = [
            vacancy_item
            for vacancy_item in employer_vacancies
            if is_valid_vacancy(vacancy_item)
        ]
        return valid_vacancies

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении вакансий для работодателя с ID {employer_id}: {e}")
        return []


def is_valid_vacancy(vacancy_data: Dict[str, Any]) -> bool:
    """Проверяет, содержит ли вакансия необходимую информацию и имеет ли валидную валюту."""
    return (
        vacancy_data.get("name") is not None
        and vacancy_data["area"].get("name") is not None
        and vacancy_data.get("salary") is not None
        and vacancy_data["salary"].get("currency") is not None
        and vacancy_data["salary"]["currency"] == "RUR"
        and vacancy_data.get("alternate_url") is not None
        and vacancy_data["employer"].get("name") is not None
    )


def fetch_employer_name(employer_id: str) -> str | None:
    """Получает имя работодателя по его ID."""
    url = f"https://api.hh.ru/employers/{employer_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        employer_data = response.json()
        return employer_data.get("name")
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных о работодателе с ID {employer_id}: {e}")
        return None


if __name__ == "__main__":
    # Список ID конкретных работодателей
    specific_employer_ids = [
        "80",
        "1740",
        "2460946",
        "15478",
        "4233",
        "59",
        "1102601",
        "208707",
        "1373",
        "106571",
    ]

    all_fetched_vacancies = fetch_vacancies_for_specific_employers(
        specific_employer_ids
    )
    if all_fetched_vacancies:
        for fetched_vacancy in all_fetched_vacancies:
            print(fetched_vacancy)
    else:
        print("Не удалось получить вакансии для указанных работодателей.")
