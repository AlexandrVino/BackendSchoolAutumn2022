from aiohttp.web_response import Response
from aiohttp_apispec import docs

from disk.api.handlers.base import BaseImportView
from disk.api.responses import ok_response


class NodeView(BaseImportView):
    URL_PATH = r'/nodes/{uid:[\w, -]+}'

    @docs(
        tags=['Default tasks'],
        summary='Добавить выгрузку с информацией о файлах/папках',
        description='''Получить информацию об элементе по идентификатору. При получении информации о папке также предоставляется информация о её дочерних элементах.

        - для пустой папки поле children равно пустому массиву, а для файла равно null
        - размер папки - это суммарный размер всех её элементов. Если папка не содержит элементов, то размер равен 0. При обновлении размера элемента, суммарный размер папки, которая содержит этот элемент, тоже обновляется.
        ''',
        responses={
            200: {'description': 'Информация об элементе.'},
            400: {'description': 'Невалидная схема документа или входные данные не верны.'},
            404: {'description': 'Элемент не найден.'},
        }
    )
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения дерева элемента
        """

        return ok_response(body=await self.get_obj_tree())
