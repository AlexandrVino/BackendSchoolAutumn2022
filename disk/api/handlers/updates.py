from datetime import timedelta
from http import HTTPStatus
from json import dumps
from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from disk.api.responses import bad_response, ok_response
from disk.db.schema import history_table, units_table
from disk.api.utils import datetime_to_str, edit_json_to_answer, str_to_datetime
from disk.api.handlers.base import BaseImportView


class UpdatesView(BaseImportView):
    URL_PATH = r'/updates'

    @docs(summary='Отобразить файлы, размер которых менялся за последние 24 часа')
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения элемента (-ов), цена которых менялась за последние 24 часа
        """

        try:
            date = str_to_datetime(self.kwargs['date'][0])
        except (ValueError, KeyError) as err:
            return bad_response(description=err)

        sql_request = units_table.select().where(
            and_(
                units_table.c.type == 'file',
                units_table.c.uid.in_(
                    select(history_table.c.uid).where(
                        and_(
                            history_table.c.update_date >= date - timedelta(days=1),
                            history_table.c.update_date <= date,
                        )
                    )
                )
            )
        )

        data = {'items': list(map(dict, await self.pg.fetch(sql_request)))}
        for record in data['items']:
            record['date'] = datetime_to_str(record['date'])

        return ok_response(body=await edit_json_to_answer(data))