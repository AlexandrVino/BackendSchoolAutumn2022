import json
from datetime import datetime


async def edit_json_to_answer(data: dict | list) -> dict:
    """
    :param data: данные для запросов
    :return: данные, подготовленные для отправки

    Функция изменения данных для ответа
    """
    return json.loads(
        json.dumps(data, ensure_ascii=False)
        .replace('"file"', '"FILE"').replace('"folder"', '"FOLDER"')
        .replace('uid', 'id').replace('parent_id', 'parentId')
    )


def datetime_to_str(date: datetime) -> str:
    """
    :param date: datetime объект
    :return: дата и время строкой

    Функция перевода datetime объект в строку
    """

    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def str_to_datetime(date: str) -> datetime:
    """
    :param date: дата и время строкой
    :return: datetime объект

    Функция перевода строки в datetime объект
    """

    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
