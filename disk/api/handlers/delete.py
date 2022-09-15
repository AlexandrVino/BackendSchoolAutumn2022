from aiohttp.web_response import Response
from aiohttp_apispec import docs

from disk.api.handlers.base import BaseImportView
from disk.api.responses import bad_response, not_found_response, ok_response
from disk.api.utils import str_to_datetime
from disk.api.pg_utils import get_item_tree, SQL_REQUESTS


class DeleteView(BaseImportView):
    URL_PATH = r'/delete/{uid:[\w, -]+}'

    @docs(
        tags=['Default tasks'],
        summary='Удалить объект со всеми дочерними',
        description='Удалить элемент по идентификатору. '
                    'При удалении папки удаляются все дочерние элементы.'
                    ' Доступ к истории обновлений удаленного элемента невозможен.\n\n'
                    ' **Обратите, пожалуйста, внимание на этот обработчик.'
                    ' При его некорректной работе тестирование может быть невозможно.**',
        responses={
            200: {'description': 'Удаление прошло успешно.'},
            400: {'description': 'Невалидная схема документа или входные данные не верны.'},
            404: {'description': 'Элемент не найден.'},
        }
    )
    async def delete(self) -> Response:
        """
        :return: Response
        Метод удаления элемента с каким-либо id из всех таблиц
        """

        ides_to_req, _ = await get_item_tree(self.uid, self.pg)

        try:
            data = str_to_datetime(self.kwargs.get('date')[0])
        except (ValueError, IndexError, TypeError) as err:
            return bad_response(description=err)

        if not data or not self.uid:
            return bad_response(
                description='Incorrect parameters '
                            '(you should pass id and date)'
            )

        if not ides_to_req:
            return not_found_response()

        ides_to_req = tuple(ides_to_req)
        sql_request = SQL_REQUESTS['delete_by_ides'].format(
            ides_to_req, ides_to_req, ides_to_req, ides_to_req
        ).replace(',)', ')')
        await self.pg.execute(sql_request)

        return ok_response()
