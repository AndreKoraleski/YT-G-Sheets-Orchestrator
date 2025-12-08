from dataclasses import dataclass
import os

from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

@dataclass(frozen=True)
class Config:
    """
    Configurações para o trabalhador ORC, obtidas de variáveis de ambiente.
    
    Attributes:
        worker_name (str | None): Nome do trabalhador, obtido da variável de ambiente WORKER_NAME.
        spreadsheet_id (str | None): ID da planilha, obtido da variável de ambiente SPREADSHEET_ID.
        service_account_file (str | None): Caminho para o arquivo de conta de serviço, obtido da variável de ambiente SERVICE_ACCOUNT_FILE.
    """
    worker_name: str | None = None
    spreadsheet_id: str | None = None
    service_account_file: str | None = None

    def __post_init__(self):
        if self.worker_name is None:
            object.__setattr__(self, 'worker_name', os.getenv('WORKER_NAME'))
        if self.spreadsheet_id is None:
            object.__setattr__(self, 'spreadsheet_id', os.getenv('SPREADSHEET_ID'))
        if self.service_account_file is None:
            object.__setattr__(self, 'service_account_file', os.getenv('SERVICE_ACCOUNT_FILE'))

        if not self.worker_name:
            # Garante que cada worker tenha identificável, mesmo que não único
            raise ValueError("A variável de ambiente 'WORKER_NAME' é obrigatória.")
        if not self.spreadsheet_id:
            raise ValueError("A variável de ambiente 'SPREADSHEET_ID' é obrigatória.")
        if not self.service_account_file:
            raise ValueError("A variável de ambiente 'SERVICE_ACCOUNT_FILE' é obrigatória.")