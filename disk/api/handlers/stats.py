from http import HTTPStatus
from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from disk.db.schema import history_table, units_table
from disk.api.utils import datetime_to_str, edit_json_to_answer, str_to_datetime
from disk.api.handlers.base import BaseImportView


class StatsView(BaseImportView):
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
        except (ValueError, KeyError):
            return Response(status=HTTPStatus.BAD_REQUEST)

        # sql получения истории обновления цены товара/категории
        sql_request = select(history_table).where(
            and_(
                date_start <= history_table.c.update_date,
                history_table.c.update_date <= date_end,
                history_table.c.uid == self.uid
            )
        )

        sizes = [[record.get('size'), record.get('update_date')] for record in await self.pg.fetch(sql_request)]

        # получаем сам объект и добавляем его историю в обновлений
        ans = await self.pg.fetchrow(
            units_table.select().where(units_table.c.uid == self.uid))
        ans = ans and dict(ans)
        if ans is None:
            raise HTTPNotFound()
        del ans['date']
        ans['stats'] = [{'update_date': datetime_to_str(update_date), 'size': size} for size, update_date in sizes]
        ans['size'] = ans['stats'][-1]['size'] if ans['stats'] else None

        return Response(body=await edit_json_to_answer(ans))
