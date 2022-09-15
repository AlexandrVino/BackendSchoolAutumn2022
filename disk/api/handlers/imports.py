import logging
from typing import Generator

from aiohttp.web_response import Response
from aiohttp_apispec import docs
from aiomisc import chunk_list
from asyncpg import Connection
from sqlalchemy.dialects.postgresql import insert

from disk.api.handlers.base import BaseView
from disk.api.responses import bad_response, ok_response
from disk.api.utils import str_to_datetime
from disk.api.validators import validate_all_items
from disk.db.schema import relations_table, units_table
from disk.utils.pg import MAX_QUERY_ARGS
from disk.api.pg_utils import (
    add_history, SQL_REQUESTS, update_parent_branch_date
)

log = logging.getLogger(__name__)


class ImportsView(BaseView):
    URL_PATH = '/imports'

    MAX_CITIZENS_PER_INSERT = MAX_QUERY_ARGS // len(units_table.columns)
    MAX_RELATIONS_PER_INSERT = MAX_QUERY_ARGS // len(relations_table.columns)
    all_insert_data = None
    need_to_update_date = None
    need_to_add_history = None

    @classmethod
    def make_units_table_rows(cls, units: list[dict], date: str) -> Generator:
        """
        :param units: список элементов для вставки
        :param date: дата обновления
        :return: Generator

        Метод, который генерирует данные готовые
         для вставки в таблицу units
        """

        for unit in units:
            yield {
                'uid': unit['id'],
                'url': unit.get('url'),
                'size': unit.get('size'),
                'date': str_to_datetime(date),
                'type': unit['type'].lower(),
                'parent_id': unit.get('parentId'),
            }

    @classmethod
    def make_relations_table_rows(cls, relations: list[dict]) -> Generator:
        """
        :param relations: список словарей для вставки
        :return: Generator

        Метод, который генерирует данные готовые
        для вставки в таблицу relations
        """

        for unit in relations:
            if not unit.get('parentId'):
                continue
            yield {
                'children_id': unit['id'],
                'relation_id': unit['parentId'],
            }

    @staticmethod
    async def add_relatives(conn: Connection, chunk: list[dict]) -> None:
        """
        :param conn: объект коннекта к бд
        :param chunk список элементов для вставки
        :return: None

        Метод, который вставляет данные в таблицу relations
        """

        query = insert(relations_table).on_conflict_do_nothing(
            index_elements=['relation_id', 'children_id']
        )
        query.parameters = []

        await conn.execute(query.values(list(chunk)))

    async def update_or_create(self, conn: Connection, chunk: list[dict]):
        """
        :param conn: объект коннекта к бд
        :param chunk список элементов для вставки
        :return: None

        Метод, который вставляет данные в таблицу units
        """

        parents = set()
        all_objects = dict()

        for data in chunk:
            if data.get('parent_id'):
                parents.add(data.get('parent_id'))
            all_objects[data.get('uid')] = data.copy()

        # проверяем, что родитель есть в бд и что его тип == 'folder'
        if parents:
            request_parents = await self.pg.fetch(
                SQL_REQUESTS['get_by_ides'].format(
                    tuple(parents)).replace(',)', ')')
            )

            for parent in request_parents:
                assert (
                        parent is not None
                        and parent.get('type').lower() == 'folder'
                ), f'Incorrect parent with id {parent.get("uid")} ' \
                   f'(Not found in db or type is FILE)'

        for data in chunk:

            # т.к. при изменении/добавлении товара
            # необходимо менять всю родительскую ветку (дату и цену)
            # добавляю в 2 списка:
            # первый для установления даты
            # на дату последнего измененного объекта
            # второй для добавления записи
            # в таблицу истории изменений (статистики)

            if data.get('parent_id'):
                self.need_to_update_date.append((data['uid'], data['date']))
            if data.get('type').lower() == 'file':
                self.need_to_add_history.append((data['uid'], data['date']))

        # добавляем объекты, которых еще нет в бд
        for obj in all_objects.values():
            print(obj)
            insert_query = insert(units_table).values(
                **obj
            ).on_conflict_do_update(index_elements=['uid'], set_=obj)
            insert_query.parameters = []
            await conn.execute(insert_query)

    @docs(summary='Добавить выгрузку с информацией о файлах/папках')
    async def post(self) -> Response:
        """
        :return: Response
        Метод добавления/изменения элемента (ов)
        """
        try:
            self.all_insert_data = {}
            self.need_to_update_date = []
            self.need_to_add_history = []

            async with self.pg.transaction() as conn:

                data = await self.request.json()

                assert data.get('items') and data.get('updateDate'), \
                    "Validation Failed (items or updateDate isn't set)"
                units = data['items']

                chunked_shop_unit_rows = list(
                    chunk_list(self.make_units_table_rows(
                        units, data['updateDate']),
                        self.MAX_CITIZENS_PER_INSERT
                    )
                )

                relations_rows = list(
                    chunk_list(
                        self.make_relations_table_rows(units),
                        self.MAX_CITIZENS_PER_INSERT
                    )
                )

                validate_all_items(chunked_shop_unit_rows)

                for chunk in chunked_shop_unit_rows:
                    await self.update_or_create(conn, chunk)
                for chunk in relations_rows:
                    await self.add_relatives(conn, chunk)

            for children_id, date in self.need_to_update_date:
                await update_parent_branch_date(children_id, self.pg, date)
            for children_id, date in self.need_to_add_history:
                await add_history(children_id, self.pg, date, {})

            return ok_response()

        except (AssertionError, ValueError, KeyError) as err:
            return bad_response(description=err)
