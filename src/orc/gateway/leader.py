import logging
import time

from gspread import Spreadsheet

from .operations import append_row, get_row, select_first_by_columns, update_row
from .worksheet import get_header_mapping, get_worksheet

logger = logging.getLogger(__name__)

ELECTION_SHEET_NAME = "Eleição de Líderes"
ELECTION_HEADER = [
    "Nome da Eleição",
    "ID do Líder",
    "Timestamp de Aquisição",
    "Timestamp de Expiração",
    "Status",
]


def _ensure_election_worksheet(spreadsheet: Spreadsheet) -> None:
    """
    Garante que a planilha de eleição de líderes exista e tenha o cabeçalho correto.

    Args:
        spreadsheet (Spreadsheet): A planilha onde a aba de eleição será verificada/criada.
    """
    return get_worksheet(
        spreadsheet=spreadsheet,
        worksheet_name=ELECTION_SHEET_NAME,
        header=ELECTION_HEADER,
        replace_header=False,
        create=True,
    )


def try_acquire_leadership(
    spreadsheet: Spreadsheet, election_name: str, worker_id: str, ttl_seconds: int = 60
) -> bool:
    """
    Tenta adquirir a liderança para uma eleição específica.

    Args:
        spreadsheet (Spreadsheet): A planilha onde a eleição de líderes ocorre.
        election_name (str): O nome da eleição para a qual o líder está sendo adquirido.
        worker_id (str): O identificador único do trabalhador que está tentando adquirir a liderança.
        ttl_seconds (int): Tempo em segundos para o qual a liderança é válida antes de expirar.

    Returns:
        bool: True se a liderança foi adquirida com sucesso, False caso contrário.
    """
    ws = _ensure_election_worksheet(spreadsheet)
    mapping = get_header_mapping(ws)

    now = time.time()
    expires_at = now + ttl_seconds

    result = select_first_by_columns(
        worksheet=ws,
        mapping=mapping,
        column_filters={"Nome da Eleição": election_name},
    )

    if result is None:
        logger.info("Eleição '%s' não existe. Criando e adquirindo liderança.", election_name)
        new_row = [""] * len(ELECTION_HEADER)
        new_row[mapping["Nome da Eleição"]] = election_name
        new_row[mapping["ID do Líder"]] = worker_id
        new_row[mapping["Timestamp de Aquisição"]] = f"{now:.6f}"
        new_row[mapping["Timestamp de Expiração"]] = f"{expires_at:.6f}"
        new_row[mapping["Status"]] = "ACTIVE"

        append_row(ws, new_row)
        return True

    row_number, row_data = result

    try:
        current_leader = row_data[mapping["ID do Líder"]]

        try:
            current_expiration = float(row_data[mapping["Timestamp de Expiração"]])
        except (ValueError, IndexError):
            current_expiration = 0.0

        should_write = False

        if current_leader == worker_id:
            logger.debug(
                "Worker '%s' já é o líder da eleição '%s'. Renovando liderança.",
                worker_id,
                election_name,
            )
            should_write = True

        elif current_expiration < now or row_data[mapping["Status"]] != "ACTIVE":
            logger.info(
                "Liderança expirada ou inativa para a eleição '%s'. Worker '%s' adquirindo liderança.",
                election_name,
                worker_id,
            )
            should_write = True

        else:
            logger.info(
                "Eleição '%s' já possui líder ativo: '%s'. Worker '%s' não pode adquirir liderança.",
                election_name,
                current_leader,
                worker_id,
            )
            return False

        if should_write:
            row_data[mapping["ID do Líder"]] = worker_id
            row_data[mapping["Timestamp de Aquisição"]] = f"{now:.6f}"
            row_data[mapping["Timestamp de Expiração"]] = f"{expires_at:.6f}"
            row_data[mapping["Status"]] = "ACTIVE"

            update_row(ws, row_number, row_data)

            time.sleep(1.0)
            check_row = get_row(ws, row_number)
            if check_row and check_row[mapping["ID do Líder"]] == worker_id:
                logger.info(
                    "Worker '%s' adquiriu liderança da eleição '%s' com sucesso.",
                    worker_id,
                    election_name,
                )
                return True

        return False

    except Exception as e:
        logger.error(
            "Erro ao tentar adquirir liderança para a eleição '%s': %s",
            election_name,
            str(e),
            exc_info=True,
        )
        return False


def release_leadership(spreadsheet: Spreadsheet, election_name: str, worker_id: str) -> None:
    """
    Libera a liderança de uma eleição específica voluntariamente.
    Isso permite que outros workers assumam imediatamente sem esperar o TTL expirar.

    Args:
        spreadsheet (Spreadsheet): A planilha onde a eleição ocorre.
        election_name (str): O nome da eleição a ser liberada.
        worker_id (str): O ID do trabalhador que está liberando a liderança.
    """
    try:
        ws = _ensure_election_worksheet(spreadsheet)
        mapping = get_header_mapping(ws)

        result = select_first_by_columns(
            worksheet=ws,
            mapping=mapping,
            column_filters={"Nome da Eleição": election_name},
        )

        if result:
            row_number, row_data = result

            current_leader = row_data[mapping["ID do Líder"]]

            if current_leader == worker_id:
                logger.info(
                    "Worker '%s' liberando voluntariamente a liderança de '%s'...",
                    worker_id,
                    election_name,
                )

                row_data[mapping["Status"]] = "RELEASED"
                row_data[mapping["Timestamp de Expiração"]] = "0"

                update_row(ws, row_number, row_data)

                logger.info("Liderança de '%s' liberada com sucesso.", election_name)
            else:
                logger.warning(
                    "Tentativa de liberar liderança falhou: Worker '%s' não é o líder atual de '%s' (Atual: '%s').",
                    worker_id,
                    election_name,
                    current_leader,
                )
        else:
            logger.warning(
                "Tentativa de liberar liderança para eleição inexistente: '%s'.", election_name
            )

    except Exception as e:
        logger.error(
            "Erro ao tentar liberar liderança para a eleição '%s': %s",
            election_name,
            str(e),
            exc_info=True,
        )
