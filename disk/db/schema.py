from enum import Enum, unique

from sqlalchemy import (Column, DateTime, Enum as PgEnum, Integer, MetaData, String, Table, UniqueConstraint)

convention = {
    'all_column_names': lambda constraint, table: '_'.join([
        column.name for column in constraint.columns.values()
    ]),
    'ix': 'ix__%(table_name)s__%(all_column_names)s',  # Именование индексов
    'uq': 'uq__%(table_name)s__%(all_column_names)s',  # Именование уникальных индексов
    'ck': 'ck__%(table_name)s__%(constraint_name)s',  # Именование CHECK-constraint-ов
    'fk': 'fk__%(table_name)s__%(all_column_names)s__%(referred_table_name)s',  # Именование внешних ключей
    'pk': 'pk__%(table_name)s'  # Именование первичных ключей
}
metadata = MetaData(naming_convention=convention)


@unique
class DiskType(Enum):
    file = 'FILE'
    folder = 'FOLDER'


units_table = Table(
    'units',
    metadata,

    Column('uid', String, primary_key=True),
    Column('url', String, nullable=False),
    Column('date', DateTime, nullable=False),
    Column('type', PgEnum(DiskType, name='type'), nullable=False),
    Column('size', Integer, nullable=True),
    Column('parentId', String, nullable=True),
)

relations_table = Table(
    'relations',
    metadata,

    Column('relationId', String, primary_key=True),
    Column('childrenId', String, primary_key=True),

    UniqueConstraint('relationId', 'childrenId', name='uix_pair_children_parent')
)

history_table = Table(
    'history',
    metadata,

    Column('uid', String, nullable=False),
    Column('size', Integer, nullable=False),
    Column('update_date', DateTime, nullable=False),

    UniqueConstraint('uid', 'update_date', name='uix_obj_history_change')
)
