from http import HTTPStatus
from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from disk.api.responses import bad_response, not_found_response, ok_response
from disk.db.schema import history_table, units_table
from disk.api.utils import datetime_to_str, edit_json_to_answer, str_to_datetime
from disk.api.handlers.base import BaseImportView


class HistoryView(BaseImportView):
    URL_PATH = r'/node/{uid:[\w, -]+}/history'

    @docs(summary='Отобразить историю изменения товара')
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения истории изменений элемента, цена которых менялась с date_start до date_end
        """

        try:
            date_end = str_to_datetime(self.kwargs['dateEnd'][0])
            date_start = str_to_datetime(self.kwargs['dateStart'][0])
        except (ValueError, KeyError) as err:
            return bad_response(description=err)

        # sql получения истории обновления цены товара/категории
        sql_request = select(history_table).where(
            and_(
                date_start <= history_table.c.update_date,
                history_table.c.update_date < date_end,
                history_table.c.uid == self.uid
            )
        )

        sizes = [[record.get('size'), record.get('update_date')] for record in await self.pg.fetch(sql_request)]

        # получаем сам объект и добавляем его историю в обновлений
        ans = await self.pg.fetchrow(units_table.select().where(units_table.c.uid == self.uid))

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
