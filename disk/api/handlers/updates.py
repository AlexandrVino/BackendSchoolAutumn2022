from datetime import timedelta

from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from disk.api.handlers.base import BaseImportView
from disk.api.responses import bad_response, ok_response
from disk.api.utils import (
    datetime_to_str, edit_json_to_answer, str_to_datetime
)
from disk.db.schema import history_table, units_table


class UpdatesView(BaseImportView):
    URL_PATH = r'/updates'

    @docs(
        tags=['Additional tasks'],
        summary='Получение списка **файлов**, которые были обновлены за последние '
                '24 часа включительно [date - 24h, date] от времени переданном в запросе.',
        description='''Получение списка **файлов**, которые были обновлены за последние 24 часа включительно [date - 24h, date] от времени переданном в запросе.
        ''',
        parameters=[
            {
                'required': False, 'name': 'dateStart',
                'type': 'string',
                'format': 'date-time',
                'example': "2022-05-28T21:12:01.000Z", 'in': 'query',
                'description': 'Дата и время начала интервала, для которого считается история. '
                               'Дата должна обрабатываться согласно ISO 8601 (такой придерживается OpenAPI). '
                               'Если дата не удовлетворяет данному формату, необходимо отвечать 400.',
            },
        ],
        responses={
            200: {'description': 'Список элементов, которые были обновлены.'},
            400: {'description': 'Невалидная схема документа или входные данные не верны.'},
        }
    )
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения элемента (-ов),
        Размер которых менялась за последние 24 часа
        """

        try:
            end_date = str_to_datetime(self.kwargs['date'][0])
            start_date = end_date - timedelta(days=1)
        except (ValueError, KeyError) as err:
            return bad_response(description=err)

        sql_request = units_table.select().where(
            and_(
                units_table.c.type == 'file',
                units_table.c.uid.in_(
                    select(history_table.c.uid).where(
                        and_(
                            history_table.c.update_date >= start_date,
                            history_table.c.update_date <= end_date,
                        )
                    )
                )
            )
        )

        data = {'items': list(map(dict, await self.pg.fetch(sql_request)))}
        for record in data['items']:
            record['date'] = datetime_to_str(record['date'])

        return ok_response(body=await edit_json_to_answer(data))
