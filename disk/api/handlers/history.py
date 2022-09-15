from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from disk.api.handlers.base import BaseImportView
from disk.api.responses import bad_response, not_found_response, ok_response
from disk.api.utils import (
    datetime_to_str, edit_json_to_answer, str_to_datetime
)
from disk.db.schema import history_table, units_table


class HistoryView(BaseImportView):
    URL_PATH = r'/node/{uid:[\w, -]+}/history'

    @docs(
        tags=['Additional tasks'],
        summary='Отобразить историю изменения товара',
        description='Получение истории обновлений по элементу за заданный полуинтервал [from, to). '
                    'История по удаленным элементам недоступна. \n\n'
                    '- размер папки - это суммарный размер всех её элементов '
                    '- можно получить статистику за всё время.*',
        parameters=[
            {
                'dateStart': 'dateStart', 'required': False, 'name': 'dateStart',
                'schema': {'type': 'string', 'format': 'date-time'},
                'example': "2022-05-28T21:12:01.000Z", 'in': 'query',
                'description': 'Дата и время начала интервала, для которого считается история. '
                               'Дата должна обрабатываться согласно ISO 8601 (такой придерживается OpenAPI). '
                               'Если дата не удовлетворяет данному формату, необходимо отвечать 400.',
            },
            {
                'dateEnd': 'dateEnd', 'required': False, 'name': 'dateEnd',
                'schema': {'type': 'string', 'format': 'date-time'},
                'description': 'Дата и время конца интервала, для которого считается история. '
                               'Дата должна обрабатываться согласно ISO 8601 (такой придерживается OpenAPI). '
                               'Если дата не удовлетворяет данному формату, необходимо отвечать 400.',
                'example': "2022-05-28T21:12:01.000Z", 'in': 'query',
            },
        ],

        responses={
            200: {'description': 'История по элементу.', 'content': 'application/json'},
            400: {'description': 'Некорректный формат запроса или некорректные даты интервала.',
                  'content': 'application/json'},
            404: {'description': 'Элемент не найден.', 'content': 'application/json'},
        }
    )
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения истории изменений элемента,
        размер которых менялась с date_start до date_end
        """

        try:
            date_end = str_to_datetime(self.kwargs['dateEnd'][0])
            date_start = str_to_datetime(self.kwargs['dateStart'][0])

            assert date_start <= date_end

        except (ValueError, KeyError, AssertionError) as err:
            return bad_response(description=err)

        # sql получения истории обновления цены товара/категории
        sql_request = select(history_table).where(
            and_(
                date_start <= history_table.c.update_date,
                history_table.c.update_date < date_end,
                history_table.c.uid == self.uid
            )
        )

        sizes = [
            [record.get('size'), record.get('update_date')]
            for record in await self.pg.fetch(sql_request)
        ]

        # получаем сам объект и добавляем его историю в обновлений
        ans = await self.pg.fetchrow(
            units_table.select().where(units_table.c.uid == self.uid)
        )

        ans = ans and dict(ans)
        if ans is None:
            return not_found_response()

        del ans['date']
        del ans['size']

        answer = {
            'items': [
                {'date': datetime_to_str(update_date), 'size': size} | ans
                for size, update_date in sizes
            ]
        }

        return ok_response(body=await edit_json_to_answer(answer))
