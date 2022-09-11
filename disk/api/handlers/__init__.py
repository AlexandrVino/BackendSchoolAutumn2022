from .delete import DeleteView
from .nodes import NodeView
from .updates import UpdatesView
from .imports import ImportsView
from .history import HistoryView

HANDLERS = (
    HistoryView, UpdatesView, NodeView, ImportsView, DeleteView
)
