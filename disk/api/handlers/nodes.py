from aiohttp.web_response import Response

from disk.api.handlers.base import BaseImportView
from disk.api.responses import ok_response


class NodeView(BaseImportView):
    URL_PATH = r'/nodes/{uid:[\w, -]+}'

    async def get(self) -> Response:
        """
        :return: Response
        Метод получения дерева элемента
        """

        return ok_response(body=await self.get_obj_tree())
