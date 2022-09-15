from aiohttp.web_response import Response
from aiohttp_apispec import docs

from disk.api.handlers.base import BaseImportView
from disk.api.responses import ok_response


class NodeView(BaseImportView):
    URL_PATH = r'/nodes/{uid:[\w, -]+}'

    @docs(summary='Получить объект со всеми дочерними')
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения дерева элемента
        """

        return ok_response(body=await self.get_obj_tree())
