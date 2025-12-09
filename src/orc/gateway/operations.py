import logging
import time

from gspread import Worksheet

from ._retry import retry


logger = logging.getLogger(__name__)


def _verify_ownership(
        worksheet: Worksheet,
        row_number: int,
        claim_column_index: int | None,
        claim_value: str | None
) -> bool:
    """
    Verifica se a linha ainda pertence ao worker (claim) antes de realizar uma operação crítica.
    Isso evita condições de corrida onde índices mudam ou tarefas são roubadas.

    Args:
        worksheet (Worksheet): A aba do Google Sheets onde a verificação será realizada.
        row_number (int): Número da linha a ser verificada (1-based).
        claim_column_index (int | None): Índice da coluna de reivindicação (0-based) ou None se não houver reivindicação.
        claim_value (str | None): Valor esperado na coluna de reivindicação ou None se não houver reivindicação.

    Returns:
        bool: True se a linha é "possuída" pelo valor esperado, False caso contrário.
    """
    if claim_column_index is None or claim_value is None:
        return True

    # Wait: Pequeno delay para permitir propagação de escritas concorrentes
    time.sleep(1.0)

    try:
        gspread_column_index = claim_column_index + 1
        current_owner = retry(lambda: worksheet.cell(row_number, gspread_column_index).value)

        if current_owner != claim_value:
            logger.warning(
                "Conflito de posse na linha %d da aba '%s'. Esperado: '%s', Encontrado: '%s'. Operação cancelada.",
                row_number, 
                worksheet.title, 
                claim_value, 
                current_owner
            )
            return False
            
        return True
    except Exception as e:
        logger.error(
            "Erro ao verificar a posse da linha %d na aba '%s': %s",
            row_number,
            worksheet.title,
            str(e),
            exc_info=True
        )
        return False


def get_column_values(
        worksheet: Worksheet,
        column_index: int,
) -> list[str]:
    """
    Retorna todos os valores de uma coluna específica.
    Ideal para carregar IDs de histórico para deduplicação em memória.

    Args:
        worksheet (Worksheet): Aba onde a coluna será lida.
        column_index (int): Índice da coluna (1-based, padrão gspread).

    Returns:
        list[str]: Lista de valores da coluna.
    """
    logger.debug("Baixando coluna %d da aba '%s'", column_index, worksheet.title)
    return retry(lambda: worksheet.col_values(column_index))


def pop_first_row_by_columns(
        worksheet: Worksheet,
        mapping: dict[str, int],
        column_filters: dict[str, str],
        claim_column: str,
        claim_value: str,
) -> tuple[int, list[str]] | None:
    """
    Busca a primeira linha que satisfaz os filtros E não tem dono,
    reserva ela (escreve claim_value) e retorna os dados.

    Args:
        worksheet (Worksheet): Aba onde a busca será realizada.
        mapping (dict[str, int]): Mapeamento {nome_coluna: indice}.
        column_filters (dict[str, str]): Filtros para busca.
        claim_column (str): Nome da coluna de claim.
        claim_value (str): Valor a ser escrito no claim.

    Returns:
        tuple[int, list[str]] | None: (row_number, row_values) ou None se não houver linha disponível.
    """
    try:
        claim_col_index = mapping[claim_column]
        # Prepara índices de filtro
        filter_indices = {mapping[k]: v for k, v in column_filters.items()}
    except KeyError as e:
        logger.error("Coluna não encontrada no mapeamento: %s", e)
        return None

    # Tenta X vezes antes de desistir (para lidar com disputas de escrita)
    for attempt in range(5):
        # 1. READ: Baixa dados para encontrar candidato
        rows = retry(lambda: worksheet.get_all_values())
        
        candidate_row_num = -1
        candidate_row_data = []

        # Itera pulando cabeçalho
        for i, row in enumerate(rows[1:], start=2):
            # Verifica se linha está vazia
            if not any(cell.strip() for cell in row):
                continue

            # Verifica filtros do usuário
            match = True
            for col_idx, val in filter_indices.items():
                if col_idx >= len(row) or row[col_idx] != val:
                    match = False
                    break
            
            if not match:
                continue

            # Verifica se JÁ está reservado (Coluna de claim deve estar vazia)
            current_claim = row[claim_col_index] if claim_col_index < len(row) else ""
            if current_claim != "" and current_claim != claim_value:
                continue  # Já tem dono

            # Achamos um candidato livre!
            candidate_row_num = i
            candidate_row_data = row
            break
        
        if candidate_row_num == -1:
            return None  # Nenhuma tarefa disponível

        # 2. CLAIM: Tenta marcar a linha
        try:
            retry(lambda: worksheet.update_cell(candidate_row_num, claim_col_index + 1, claim_value))
        except Exception:
            continue  # Erro de escrita, tenta próxima

        # 3. WAIT & VERIFY (Via helper)
        if _verify_ownership(worksheet, candidate_row_num, claim_col_index, claim_value):
            # Atualiza o dado em memória com o claim para retornar consistente
            if len(candidate_row_data) > claim_col_index:
                candidate_row_data[claim_col_index] = claim_value
            elif len(candidate_row_data) == claim_col_index:
                candidate_row_data.append(claim_value)
                
            return candidate_row_num, candidate_row_data
        
        # Se falhou, loop continua

    return None


def select_first_by_columns(
        worksheet: Worksheet,
        mapping: dict[str, int],
        column_filters: dict[str, str],
) -> tuple[int, list[str]] | None:
    """
    Seleciona a primeira linha da aba que satisfaça todos os filtros de coluna,
    usando nomes de coluna via mapping.

    Args:
        worksheet (Worksheet): Aba onde a busca será realizada.
        mapping (dict[str, int]): Mapeamento {nome_coluna: indice}.
        column_filters (dict[str, str]): Mapeamento {nome_coluna: valor_esperado}.

    Returns:
        tuple[int, list[str]] | None:
            - (row_number, row_values) da primeira linha encontrada
            - None se nenhuma linha satisfizer os filtros
    """
    index_filters = {
        mapping[column_name]: expected_value
        for column_name, expected_value in column_filters.items()
    }

    logger.debug(
        "Selecionando a primeira linha na aba '%s' com filtros nas colunas: %s",
        worksheet.title,
        column_filters,
    )

    rows = retry(lambda: worksheet.get_all_values())

    for row_number, row in enumerate(rows[1:], start=2):
        if not row:
            continue

        matched = True
        for column_index, expected_value in index_filters.items():
            cell_value = row[column_index] if column_index < len(row) else ""

            if cell_value != expected_value:
                matched = False
                break

        if matched:
            logger.debug(
                "Linha encontrada na aba '%s': linha %d",
                worksheet.title,
                row_number,
            )
            return row_number, row

    logger.debug(
        "Nenhuma linha encontrada na aba '%s' com os filtros informados.",
        worksheet.title,
    )
    return None


def select_all_by_columns(
        worksheet: Worksheet,
        mapping: dict[str, int],
        column_filters: dict[str, str],
) -> list[tuple[int, list[str]]]:
    """
    Seleciona todas as linhas da aba que satisfaçam todos os filtros de coluna,
    usando nomes de coluna via mapping.

    Args:
        worksheet (Worksheet): Aba onde a busca será realizada.
        mapping (dict[str, int]): Mapeamento {nome_coluna: indice}.
        column_filters (dict[str, str]): Mapeamento {nome_coluna: valor_esperado}.

    Returns:
        list[tuple[int, list[str]]]:
            Lista de tuplas (row_number, row_values) para todas as linhas encontradas.
    """
    index_filters = {
        mapping[column_name]: expected_value
        for column_name, expected_value in column_filters.items()
    }

    logger.debug(
        "Selecionando todas as linhas na aba '%s' com filtros nas colunas: %s",
        worksheet.title,
        column_filters,
    )

    rows = retry(lambda: worksheet.get_all_values())

    results: list[tuple[int, list[str]]] = []

    for row_number, row in enumerate(rows[1:], start=2):
        if not row:
            continue

        matched = True
        for column_index, expected_value in index_filters.items():
            cell_value = row[column_index] if column_index < len(row) else ""

            if cell_value != expected_value:
                matched = False
                break

        if matched:
            results.append((row_number, row))

    logger.debug(
        "%d linhas encontradas na aba '%s' com os filtros informados.",
        len(results),
        worksheet.title,
    )
    return results


def append_row(
        worksheet: Worksheet,
        row: list[str],
) -> None:
    """
    Adiciona uma única linha ao final da aba.

    Args:
        worksheet (Worksheet): Aba onde a linha será adicionada.
        row (list[str]): Lista de valores da linha.
    """
    logger.debug(
        "Adicionando uma linha na aba '%s': %s",
        worksheet.title,
        row,
    )

    retry(lambda: worksheet.append_row(row))

    logger.debug(
        "Linha adicionada com sucesso na aba '%s'.",
        worksheet.title,
    )


def append_rows(
        worksheet: Worksheet,
        rows: list[list[str]],
) -> None:
    """
    Adiciona múltiplas linhas ao final da aba em uma única operação.

    Args:
        worksheet (Worksheet): Aba onde as linhas serão adicionadas.
        rows (list[list[str]]): Lista de linhas a serem adicionadas.
    """
    if not rows:
        logger.debug(
            "Nenhuma linha para adicionar na aba '%s'.",
            worksheet.title,
        )
        return

    logger.debug(
        "Adicionando %d linhas na aba '%s'.",
        len(rows),
        worksheet.title,
    )

    retry(lambda: worksheet.append_rows(rows))

    logger.debug(
        "%d linhas adicionadas com sucesso na aba '%s'.",
        len(rows),
        worksheet.title,
    )


def get_row(
        worksheet: Worksheet,
        row_number: int,
) -> list[str] | None:
    """
    Lê o conteúdo de uma linha específica da aba pelo número da linha.

    Args:
        worksheet (Worksheet): Aba onde a linha será lida.
        row_number (int): Número da linha (1-based, como no Google Sheets).

    Returns:
        list[str] | None:
            - Lista de valores da linha, se existir
            - None se a linha estiver vazia ou fora do intervalo
    """
    logger.debug(
        "Lendo a linha %d da aba '%s'.",
        row_number,
        worksheet.title,
    )

    row = retry(lambda: worksheet.row_values(row_number))

    if not row:
        logger.debug(
            "Linha %d da aba '%s' está vazia ou não existe.",
            row_number,
            worksheet.title,
        )
        return None

    return row


def get_rows(
        worksheet: Worksheet,
        start_row: int,
        end_row: int,
) -> list[list[str]]:
    """
    Lê um intervalo de linhas da aba.

    Args:
        worksheet (Worksheet): Aba onde as linhas serão lidas.
        start_row (int): Linha inicial (1-based, inclusiva).
        end_row (int): Linha final (1-based, inclusiva).

    Returns:
        list[list[str]]:
            Lista de linhas lidas. Linhas vazias são retornadas como listas vazias.
    """
    logger.debug(
        "Lendo linhas %d até %d da aba '%s'.",
        start_row,
        end_row,
        worksheet.title,
    )

    if start_row > end_row:
        return []

    cell_range = f"{start_row}:{end_row}"

    rows = retry(lambda: worksheet.get(cell_range))

    return rows


def get_rows_by_numbers(
        worksheet: Worksheet,
        row_numbers: list[int],
) -> list[list[str]]:
    """
    Lê múltiplas linhas específicas da aba a partir de uma lista de números de linha.

    Args:
        worksheet (Worksheet): Aba onde as linhas serão lidas.
        row_numbers (list[int]): Lista de números de linha (1-based).

    Returns:
        list[list[str]]:
            Lista de linhas lidas. Linhas inexistentes retornam listas vazias.
    """
    if not row_numbers:
        return []

    logger.debug(
        "Lendo %d linhas específicas da aba '%s': %s",
        len(row_numbers),
        worksheet.title,
        row_numbers,
    )

    rows: list[list[str]] = []

    for row_number in row_numbers:
        row = retry(lambda rn=row_number: worksheet.row_values(rn))
        rows.append(row)

    return rows


def get_next_valid_row(
        worksheet: Worksheet,
) -> tuple[int, list[str]] | None:
    """
    Obtém a próxima linha válida (não vazia) da aba, ignorando o cabeçalho
    e eventuais buracos no topo.

    Essa função é ideal para uso em filas baseadas em abas, onde o topo pode
    conter espaços vazios devido a movimentações anteriores.

    Args:
        worksheet (Worksheet): Aba onde a busca será realizada.

    Returns:
        tuple[int, list[str]] | None:
            - (row_number, row_values) da primeira linha não vazia encontrada
            - None se nenhuma linha válida existir
    """
    logger.debug(
        "Buscando a próxima linha válida na aba '%s'.",
        worksheet.title,
    )

    rows = retry(lambda: worksheet.get_all_values())

    for row_number, row in enumerate(rows[1:], start=2):
        if row and any(cell.strip() for cell in row):
            logger.debug(
                "Próxima linha válida encontrada na aba '%s': linha %d",
                worksheet.title,
                row_number,
            )
            return row_number, row

    logger.debug(
        "Nenhuma linha válida encontrada na aba '%s'.",
        worksheet.title,
    )
    return None


def update_row(
        worksheet: Worksheet,
        row_number: int,
        new_row: list[str],
        claim_column_index: int | None = None,
        claim_value: str | None = None,
) -> bool:
    """
    Substitui o conteúdo completo de uma linha por novos valores.

    Args:
        worksheet (Worksheet): Aba onde a linha será atualizada.
        row_number (int): Número da linha (1-based).
        new_row (list[str]): Novos valores da linha.
        claim_column_index (int | None): Índice da coluna de reivindicação (0-based) ou None se não houver reivindicação.
        claim_value (str | None): Valor esperado na coluna de reivindicação ou None se não houver reivindicação.

    Returns:
        bool: True se a linha foi atualizada com sucesso, False caso contrário.
    """
    if not _verify_ownership(worksheet, row_number, claim_column_index, claim_value):
        return False
    
    logger.debug(
        "Atualizando a linha %d da aba '%s' para: %s",
        row_number,
        worksheet.title,
        new_row,
    )

    cell_range = f"{row_number}:{row_number}"
    retry(lambda: worksheet.update(cell_range, [new_row]))

    logger.debug(
        "Linha %d atualizada com sucesso na aba '%s'.",
        row_number,
        worksheet.title,
    )
    return True


def delete_row(
        worksheet: Worksheet,
        row_number: int,
        claim_column_index: int | None = None,
        claim_value: str | None = None,
) -> bool:
    """
    "Apaga" uma linha da aba tornando todas as células dessa linha vazias
    (em vez de remover a linha do sheet).

    Args:
        worksheet (Worksheet): Aba onde a linha será limpa.
        row_number (int): Número da linha (1-based).
        claim_column_index (int | None): Índice da coluna de reivindicação (0-based) ou None se não houver reivindicação.
        claim_value (str | None): Valor esperado na coluna de reivindicação ou None se não houver reivindicação.

    Returns:
        bool: True se a linha foi limpa com sucesso, False caso contrário.
    """
    if not _verify_ownership(worksheet, row_number, claim_column_index, claim_value):
        return False

    logger.debug(
        "Limpando (soft delete) a linha %d da aba '%s'.",
        row_number,
        worksheet.title,
    )

    try:
        # Pega a quantidade de colunas da planilha para garantir que todas as células da linha sejam zeradas
        column_count = retry(lambda: worksheet.col_count)
        if column_count <= 0:
            logger.debug(
                "A aba '%s' não possui colunas conhecidas. Nada a limpar para a linha %d.",
                worksheet.title,
                row_number,
            )
            return True

        empty_row = [""] * column_count
        cell_range = f"{row_number}:{row_number}"
        retry(lambda: worksheet.update(cell_range, [empty_row]))
    except Exception as e:
        logger.error(
            "Erro ao limpar a linha %d na aba '%s': %s",
            row_number,
            worksheet.title,
            str(e),
            exc_info=True,
        )
        return False

    logger.debug(
        "Linha %d limpa com sucesso na aba '%s'.",
        row_number,
        worksheet.title,
    )
    return True


def move_row(
        source_ws: Worksheet,
        target_ws: Worksheet,
        row_number: int,
        claim_column_index: int | None = None,
        claim_value: str | None = None,
) -> list[str] | None:
    """
    Copia a linha para o destino e realiza SOFT DELETE na origem (limpa conteúdo).
    Isso é vital para não alterar os índices das linhas de outros workers.

    A operação consiste em:
        - verificar posse (claim)
        - ler a linha da aba de origem
        - append na aba de destino
        - limpar conteúdo na aba de origem (soft delete)

    Se a limpeza falhar após o append, tenta rollback no destino.

    Args:
        source_ws (Worksheet): Aba de origem.
        target_ws (Worksheet): Aba de destino.
        row_number (int): Número da linha a ser movida (1-based).
        claim_column_index (int | None): Índice da coluna de claim (0-based).
        claim_value (str | None): Valor esperado no claim.

    Returns:
        list[str] | None:
            Conteúdo da linha movida, ou None se a linha estiver vazia ou houver conflito.
    """
    # Verificação de posse
    if not _verify_ownership(source_ws, row_number, claim_column_index, claim_value):
        return None

    logger.debug("Movendo linha %d de '%s' para '%s' (Soft Delete).", row_number, source_ws.title, target_ws.title)

    # 1. Ler dados
    row_values = retry(lambda: source_ws.row_values(row_number))
    if not row_values:
        return None

    # Validação extra de claim em memória
    if claim_column_index is not None and claim_value is not None:
        if len(row_values) > claim_column_index:
            if row_values[claim_column_index] != claim_value:
                logger.warning("Abortando move: Dado lido difere do claim esperado.")
                return None

    # 2. Append no Destino
    retry(lambda: target_ws.append_row(row_values))

    # 3. Soft Delete na Origem (Update com strings vazias)
    try:
        # Tenta pegar número de colunas para criar a linha vazia exata
        col_count = len(row_values)
        empty_row = [""] * col_count
        
        cell_range = f"{row_number}:{row_number}"
        retry(lambda: source_ws.update(cell_range, [empty_row]))
    except Exception:
        # Rollback simples: tenta remover do destino para não duplicar
        logger.error("Falha ao limpar linha %d. Tentando rollback no destino.", row_number, exc_info=True)
        try:
            last = retry(lambda: target_ws.row_count)
            retry(lambda: target_ws.delete_rows(last))
        except Exception:
            logger.critical("FALHA DE ROLLBACK. Possível duplicata no histórico.")
        raise

    return row_values
