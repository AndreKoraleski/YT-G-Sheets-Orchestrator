"""
Gateway para acesso ao Google Sheets.

Este módulo encapsula todas as operações de leitura e escrita na API do Google Sheets,
fornecendo uma interface unificada e com retry automático.

Módulos:
    - connection: Conexão e obtenção de spreadsheets
    - worksheet: Gerenciamento de abas (worksheets)
    - operations: Operações CRUD em linhas
"""

from ._retry import configure_rate_limiting, update_active_workers
from .connection import get_spreadsheet
from .leader import (
    release_leadership,
    try_acquire_leadership,
)
from .operations import (
    append_row,
    append_rows,
    delete_row,
    get_column_values,
    get_next_valid_row,
    get_row,
    get_rows,
    get_rows_by_numbers,
    move_row,
    pop_first_row_by_columns,
    select_all_by_columns,
    select_first_by_columns,
    update_row,
)
from .worksheet import get_header_mapping, get_worksheet

__all__ = [
    "get_spreadsheet",
    "get_worksheet",
    "get_header_mapping",
    "get_column_values",
    "pop_first_row_by_columns",
    "select_first_by_columns",
    "select_all_by_columns",
    "append_row",
    "append_rows",
    "get_row",
    "get_rows",
    "get_rows_by_numbers",
    "get_next_valid_row",
    "update_row",
    "delete_row",
    "move_row",
    "try_acquire_leadership",
    "release_leadership",
    "configure_rate_limiting",
    "update_active_workers",
]
