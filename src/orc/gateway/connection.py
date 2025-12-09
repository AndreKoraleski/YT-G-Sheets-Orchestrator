import logging

from gspread import Client, Spreadsheet, SpreadsheetNotFound
from google.oauth2.service_account import Credentials

from ._retry import retry


logger = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
]


def _connect_service_account(service_account_file: str) -> Client:
    """
    Conecta-se à API do Google Sheets usando um arquivo de conta de serviço.
    
    Args:
        service_account_file (str): Caminho para o arquivo de conta de serviço JSON.

    Returns:
        Client: Cliente autenticado do gspread para interagir com a API do Google Sheets.
    """
    logger.debug("Conectando à API do Google Sheets usando: %s", service_account_file)
    credentials = Credentials.from_service_account_file(
        service_account_file,
        scopes=SCOPES,
    )
    client = Client(auth=credentials)
    logger.info("Conexão estabelecida com sucesso à API do Google Sheets.")
    return client


def get_spreadsheet(spreadsheet_id: str, service_account_file: str) -> Spreadsheet:
    """
    Obtém uma planilha do Google Sheets pelo seu ID.

    Args:
        spreadsheet_id (str): ID da planilha do Google Sheets.
        service_account_file (str): Caminho para o arquivo de conta de serviço JSON.

    Returns:
        Spreadsheet: Objeto da planilha obtida.
    """
    try:
        logger.debug("Obtendo a planilha com ID: %s", spreadsheet_id)
        client = _connect_service_account(service_account_file)
        spreadsheet = retry(lambda: client.open_by_key(spreadsheet_id))
        logger.info("Planilha obtida com sucesso: %s", spreadsheet.title)
        return spreadsheet

    except SpreadsheetNotFound:
        logger.error("Planilha com ID %s não encontrada.", spreadsheet_id)
        raise
