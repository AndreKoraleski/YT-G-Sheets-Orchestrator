import logging

from gspread import Spreadsheet, Worksheet, WorksheetNotFound

from ._retry import retry

logger = logging.getLogger(__name__)


def _enforce_header(worksheet: Worksheet, expected_header: list[str], replace_header: bool) -> None:
    """
    Substitui o cabeçalho de uma aba do Google Sheets por um cabeçalho esperado.

    Args:
        worksheet (Worksheet): A aba do Google Sheets onde o cabeçalho será substituído.
        expected_header (list[str]): Lista de strings representando o cabeçalho esperado.
    """
    logger.debug("Substituindo o cabeçalho da aba '%s'.", worksheet.title)
    if replace_header:
        retry(lambda: worksheet.delete_rows(1))
        retry(lambda: worksheet.insert_row(expected_header, index=1))
        logger.info("Cabeçalho substituído com sucesso na aba '%s'.", worksheet.title)
    else:
        if retry(lambda: worksheet.row_values(1)) != expected_header:
            logger.error("Cabeçalho da aba '%s' não corresponde ao esperado.", worksheet.title)
            raise ValueError(f"O cabeçalho da aba '{worksheet.title}' não corresponde ao esperado.")


def get_header_mapping(worksheet: Worksheet) -> dict[str, int]:
    """
    Obtém um mapeamento do cabeçalho de uma aba do Google Sheets, associando nomes de colunas aos seus índices.

    Args:
        worksheet (Worksheet): A aba do Google Sheets da qual o cabeçalho será obtido.

    Returns:
        dict[str, int]: Dicionário mapeando nomes de colunas para seus índices.
    """
    logger.debug("Obtendo o mapeamento de cabeçalho para a aba '%s'.", worksheet.title)

    header = retry(lambda: worksheet.row_values(1))

    mapping: dict[str, int] = {}

    for index, column_name in enumerate(header):
        if column_name in mapping:
            raise ValueError(f"Nome de coluna duplicado encontrado no cabeçalho: '{column_name}'")
        mapping[column_name] = index

    logger.debug("Mapeamento de cabeçalho obtido para a aba '%s': %s", worksheet.title, mapping)
    return mapping


def _create_worksheet(
    spreadsheet: Spreadsheet, worksheet_name: str, header: list[str]
) -> Worksheet:
    """
    Cria uma nova aba em uma planilha do Google Sheets com um cabeçalho especificado.

    Args:
        spreadsheet (Spreadsheet): A planilha onde a nova aba será criada.
        worksheet_name (str): Nome da nova aba a ser criada.
        header (list[str]): Lista de strings representando o cabeçalho da nova aba.

    Returns:
        Worksheet: A aba criada.
    """
    logger.debug("Criando a aba '%s' na planilha '%s'.", worksheet_name, spreadsheet.title)
    worksheet = retry(
        lambda: spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols=str(len(header)))
    )
    if header:
        retry(lambda: worksheet.insert_row(header, index=1))
    logger.info("Aba criada com sucesso: %s", worksheet.title)
    return worksheet


def get_worksheet(
    spreadsheet: Spreadsheet,
    worksheet_name: str,
    header: list[str],
    replace_header: bool,
    create: bool,
) -> Worksheet:
    """
    Obtém uma aba de uma planilha do Google Sheets, com opções para validar ou criar a aba.

    Args:
        spreadsheet (Spreadsheet): A planilha do Google Sheets onde a aba será obtida.
        worksheet_name (str): Nome da aba a ser obtida.
        header (list[str]): Lista de strings representando o cabeçalho esperado.
        replace_header (bool): Indica se o cabeçalho existente deve ser substituído pelo esperado.
        create (bool): Indica se a aba deve ser criada se não existir.

    Returns:
        Worksheet: A aba obtida ou criada.
    """

    try:
        logger.debug("Obtendo a aba '%s' da planilha '%s'.", worksheet_name, spreadsheet.title)
        worksheet = retry(lambda: spreadsheet.worksheet(worksheet_name))

        if header:
            _enforce_header(worksheet, header, replace_header)

        logger.info("Aba obtida com sucesso: %s", worksheet.title)
        return worksheet

    except WorksheetNotFound:
        logger.warning(
            "Aba '%s' não encontrada na planilha '%s'.", worksheet_name, spreadsheet.title
        )
        if create:
            return _create_worksheet(spreadsheet, worksheet_name, header)
        raise
