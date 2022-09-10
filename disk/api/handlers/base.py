import logging
from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_urldispatcher import View
from asyncpgsa import PG

from disk.api.utils import get_obj_tree_by_id

log = logging.getLogger(__name__)


class BaseView(View):
    """
    Базовый класс обработчика запросов
    """

    URL_PATH: str

    @property
    def pg(self) -> PG:
        log.debug('Registering handler %r as %r', self.request.app.keys(), type(self.request.app))
        return self.request.app['pg']

    async def get_obj(self, query):
        return await self.pg.fetchrow(query)


class BaseImportView(BaseView):
    """
    Базовый класс обработчика запросов с параметром-id в url
    """

    @property
    def uid(self) -> str:
        return str(self.request.match_info.get('uid'))

    @property
    def kwargs(self) -> dict:
        return parse_qs(urlparse(unquote(str(self.request.url))).query)

    async def get_obj_tree(self) -> dict:
        """
        :return: dict - дерево, в котором текущий является корнем
        """

        return await get_obj_tree_by_id(self.uid, self.pg)
