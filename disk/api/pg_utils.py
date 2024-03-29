from datetime import datetime

from aiohttp.web_exceptions import HTTPNotFound
from aiomisc import chunk_list
from asyncpg import Record
from asyncpgsa import PG
from sqlalchemy.dialects.postgresql import insert

from disk.api.utils import datetime_to_str, edit_json_to_answer
from disk.db.schema import history_table
from disk.utils.pg import MAX_QUERY_ARGS

'''
Пишу ручками некоторые запросы т.к.
1) либо sqlalchemy генерит что-то а потом на это и ругается,
(при использовании Функции "_in" )
2) либо это обновление множества записей сразу
(не нагуглил нормальное решение :/ )
3) либо это рекурсивный запрос, который легче написать ручками
'''

SQL_REQUESTS = {
    'get_by_ides': '''        SELECT * FROM units WHERE units.uid IN {}''',
    'delete_by_ides': '''
    DELETE FROM public.history WHERE uid IN {};
    DELETE FROM public.units WHERE uid IN {};
    DELETE FROM public.relations WHERE children_id
    IN {} OR relation_id IN {};''',
    'get_item_tree': '''
    WITH RECURSIVE search_tree(relation_id, children_id) AS (
        SELECT t.relation_id, t.children_id
        FROM relations t WHERE relation_id = '{}'
      UNION ALL
        SELECT t.relation_id, t.children_id
        FROM relations t, search_tree st
        WHERE t.relation_id = st.children_id
    )
    SELECT * FROM search_tree;''',
    'get_parent_brunch': '''
    WITH RECURSIVE search_tree(relation_id, children_id) AS (
        SELECT t.relation_id, t.children_id
        FROM relations t WHERE children_id = '{}'
      UNION ALL
        SELECT t.relation_id, t.children_id
        FROM relations t, search_tree st
        WHERE t.children_id = st.relation_id
    )
    SELECT * FROM search_tree;''',
    'update_date': '''
    UPDATE units
    SET date = '{}'
    WHERE units.uid IN {}''',
}


async def get_obj_tree_by_id(uid: str, pg: PG) -> dict:
    """
    :param uid: id элемента, для которого надо создать дерево
    :param pg: PG объект коннекта к базе данных
    :return: json, готовый к отправке на клиент

    Функция, возвращающая json, готовый к отправке на клиент
    """

    ides_to_req, ides = await get_item_tree(uid, pg)
    if not ides_to_req:
        raise HTTPNotFound()

    sql_request = SQL_REQUESTS['get_by_ides'].format(
        tuple(ides_to_req)
    ).replace(',)', ')')

    records = await pg.fetch(sql_request)
    records = {record.get('uid'): dict(record) for record in records}
    ans = records.get(uid)
    ans['date'] = datetime_to_str(ans['date'])

    await build_tree_json(ans, ides, records)
    await get_total_size(ans)

    return await edit_json_to_answer(ans)


async def get_history(
        obj_tree: dict,
        update_date: datetime,
        ides: list,
        data: dict
):
    """
    :param obj_tree: дерево элементов, для которых необходимо получить историю
    :param update_date: время обновления
    :param ides: список айдишников для запроса
    :param data: словарь историй
    :return: None

    Функция, которая рекурсивно собирает историю
    """

    record = ides.pop(0)
    parent_id, children_id = (
        record.get('relation_id'),
        record.get('children_id')
    )

    data[parent_id] = {
        'size': obj_tree['size'],
        'date': update_date
    }

    for children in obj_tree['children']:
        if children.get('id') != children_id:
            continue
        if not ides:
            data[children_id] = {
                'size': children['size'],
                'date': update_date
            }
        else:
            await get_history(children, update_date, ides, data)


async def get_parent_brunch_ides(children_id: str, pg: PG) -> list[Record]:
    """
    :param children_id: id дочернего элемента в ветке
    :param pg: PG объект коннекта к базе данных
    :return: список рекордов

    Функция, возвращающая объекты из всей родительской ветки
    """

    return await pg.fetch(
        SQL_REQUESTS['get_parent_brunch'].format(children_id)
    )


def get_history_table_chunk(sizes: dict):
    """
    :param sizes: Словарь со входными данными
    :return: None

    Функция генерации данных для записи в таблицу историй
    """

    for obj_id, obj_data in sizes.items():
        yield {
            'uid': obj_id,
            'update_date': obj_data.get('date'),
            'size': obj_data.get('size'),
        }


async def add_history(
        children_id: str,
        pg: PG,
        update_date: datetime,
        main_parents_trees: dict
) -> None:
    """
    :param children_id: id дочернего элемента в ветке
    :param pg: PG объект коннекта к базе данных
    :param update_date: время обновления
    :param main_parents_trees: словарь,
    чтобы не запрашивать историю и дерево объекта несколько раз
    :return: None

    Функция вычисления и добавления истории объекту
    """

    ides = await get_parent_brunch_ides(children_id, pg)
    main_parent_id = ides and ides[-1].get('relation_id')

    if main_parents_trees.get(main_parent_id) is None:
        sizes = {}
        main_parent_tree = await get_obj_tree_by_id(
            main_parent_id or children_id, pg
        )
        await get_history(main_parent_tree, update_date, ides[::-1], sizes)

        main_parents_trees[main_parent_id] = [main_parent_tree, sizes]

        sql_request = insert(history_table).on_conflict_do_nothing(
            index_elements=['uid', 'update_date']
        )
        sql_request.parameters = []

        history_rows = list(chunk_list(
            get_history_table_chunk(sizes), MAX_QUERY_ARGS // 3))
        for chunk in history_rows:
            await pg.execute(sql_request.values(chunk))


async def update_parent_branch_date(
        children_id: str,
        pg: PG,
        update_date: datetime
) -> None:
    """
    :param children_id: id дочернего элемента в ветке
    :param pg: PG объект коннекта к базе данных
    :param update_date: время обновления
    :return: None

    Функция обновляет дату во всей родительской ветке
    """

    ides = await get_parent_brunch_ides(children_id, pg)
    ides_to_req = set()

    for record in ides:
        ides_to_req.update((
            record.get('relation_id'),
            record.get('children_id')
        ))

    if ides_to_req:
        sql_request = SQL_REQUESTS['update_date'].format(
            update_date, tuple(ides_to_req)
        )
        await pg.execute(sql_request)


async def get_item_tree(
        root_id, pg: PG
) -> tuple[set[str], list[Record]] | tuple[None, None]:
    """
    :param root_id: id корневого элемента дерева
    :param pg: PG объект коннекта к базе данных
    :return:
    """

    sql_request = SQL_REQUESTS['get_item_tree'].format(root_id)

    ides = await pg.fetch(sql_request)

    if not ides:
        return None, None

    ides_to_req = {root_id}

    for record in ides:
        ides_to_req.update(record.values())

    return ides_to_req, ides


async def build_tree_json(
        ans: dict,
        data: list[Record],
        records: dict[str: dict]
) -> None:
    """
    :param ans: Словарь-ответ
    :param data: список рекордов для построения дерева (таблица связей)
    :param records: словарь id: record для удобства
    :return: None

    Функция построения базового дерева ответа
    """

    while data:

        # костыль во избежание зацикливания
        # если среди дочерних элементов текущего нет не одного из data
        # мы прерываем цикл
        any_children_in_data = True

        for index, record in enumerate(data):
            parent_id, children_id = record.get('relation_id'), \
                                     record.get('children_id')
            if parent_id != ans['uid']:
                continue

            any_children_in_data = False
            data.pop(index)

            if ans.get('children') is None:
                ans['children'] = []
            ans['children'].append(records[children_id])

            await build_tree_json(ans['children'][-1], data, records)

        if any_children_in_data:
            break


async def get_total_size(tree: dict) -> int | None:
    """
    :param tree: дерево элементов
    :return: либо цену и кол-во дочерних элементов (для рекурсии), либо None

    Функция, считающая и добавляющая цены категории
    """

    size = tree.get('size') or 0

    if tree.get('children'):
        for item in tree['children']:
            if item.get('children'):

                local_size = await get_total_size(item)
                item['size'] = local_size
                size += local_size

            else:
                item['children'] = None
                size += 0 if item.get('size') is None else item.get('size')

            item['date'] = datetime_to_str(item['date'])

        tree['size'] = size
    else:
        tree['date'] = datetime_to_str(tree['date'])
        tree['children'] = None

    return size
