"""
Definição de schemas para Sources (fontes de conteúdo).

Define estruturas de dados para as 3 tabelas do pipeline:
- Sources: Fila de fontes a serem processadas (requer eleição de líder)
- Sources History: Registro de fontes processadas com sucesso
- Sources DLQ: Dead Letter Queue para fontes que falharam
"""

import uuid
from dataclasses import dataclass
from datetime import datetime

# ============================================================================
# SOURCES
# ============================================================================

SOURCES_TABLE_NAME = "Sources"
SOURCES_TABLE_HEADER = [
    "ID",
    "URL",
    "Nome",
    "Quantidade de Vídeos",
    "Timestamp de Reivindicação",
    "Timestamp de Conclusão",
    "Status",
    "Worker Atribuído",
]


@dataclass
class SourceEntry:
    """
    Estrutura de dados para uma fonte na fila de processamento.

    Inicialmente criada apenas com URL. Metadados são preenchidos
    via yt-dlp quando a fonte é reivindicada pelo worker.

    Attributes:
        source_id (str): Identificador único da fonte.
        url (str): URL da fonte (único campo obrigatório na criação).
        name (str): Nome da fonte extraído via yt-dlp.
        video_count (str): Quantidade de vídeos encontrados.
        claimed_at (str): Timestamp de reivindicação (ISO 8601).
        completed_at (str): Timestamp de conclusão (ISO 8601).
        status (str): Estado atual (PENDING, CLAIMED, EXTRACTING, COMPLETED, FAILED).
        assigned_worker (str): Nome do worker que reivindicou (claim column).
    """

    url: str
    source_id: str = ""
    name: str = ""
    video_count: str = "0"
    claimed_at: str = ""
    completed_at: str = ""
    status: str = "PENDING"
    assigned_worker: str = ""

    def __post_init__(self):
        """
        Gera source_id automaticamente se não fornecido.
        """
        if not self.source_id:
            self.source_id = str(uuid.uuid4())

    def claim(self, worker_name: str) -> None:
        """
        Reivindica a fonte para um worker específico.

        Args:
            worker_name (str): Nome do worker que está reivindicando.
        """
        self.assigned_worker = worker_name
        self.claimed_at = datetime.now().isoformat()
        self.status = "CLAIMED"

    def update_from_ytdlp(self, info_dict: dict) -> None:
        """
        Atualiza metadados da fonte extraídos via yt-dlp.

        Args:
            info_dict (dict): Dicionário retornado por yt_dlp.extract_info().
        """
        # Extrai nome da fonte
        if "title" in info_dict:
            self.name = info_dict["title"]
        elif "channel" in info_dict:
            self.name = info_dict["channel"]
        elif "uploader" in info_dict:
            self.name = info_dict["uploader"]
        else:
            self.name = "Unknown Source"

        # Conta vídeos (se for playlist/channel)
        if "entries" in info_dict:
            self.video_count = str(len(list(info_dict["entries"])))
        else:
            self.video_count = "1"

        self.status = "EXTRACTING"

    def complete(self) -> None:
        """
        Marca a fonte como completada com sucesso.
        """
        self.completed_at = datetime.now().isoformat()
        self.status = "COMPLETED"

    def fail(self) -> None:
        """
        Marca a fonte como falhada.
        """
        self.completed_at = datetime.now().isoformat()
        self.status = "FAILED"

    def to_row(self) -> list[str]:
        """
        Converte para lista de strings no formato da tabela.

        Returns:
            list[str]: Lista de valores para inserir no Google Sheets.
        """
        return [
            self.source_id,
            self.url,
            self.name,
            self.video_count,
            self.claimed_at,
            self.completed_at,
            self.status,
            self.assigned_worker,
        ]

    @classmethod
    def from_url(cls, url: str) -> "SourceEntry":
        """
        Cria uma fonte minimalista a partir de apenas uma URL.

        Args:
            url (str): URL da fonte.

        Returns:
            SourceEntry: Instância criada.
        """
        return cls(url=url)

    @classmethod
    def from_row(cls, row_data: list[str]) -> "SourceEntry":
        """
        Reconstrói instância a partir de dados do Sheets.

        Args:
            row_data (list[str]): Lista de valores de uma linha do Google Sheets.

        Returns:
            SourceEntry: Instância reconstruída.
        """
        return cls(
            url=row_data[1] if len(row_data) > 1 else "",
            source_id=row_data[0] if len(row_data) > 0 else "",
            name=row_data[2] if len(row_data) > 2 else "",
            video_count=row_data[3] if len(row_data) > 3 else "0",
            claimed_at=row_data[4] if len(row_data) > 4 else "",
            completed_at=row_data[5] if len(row_data) > 5 else "",
            status=row_data[6] if len(row_data) > 6 else "PENDING",
            assigned_worker=row_data[7] if len(row_data) > 7 else "",
        )


# ============================================================================
# SOURCES HISTORY
# ============================================================================

SOURCES_HISTORY_TABLE_NAME = "Sources History"
SOURCES_HISTORY_TABLE_HEADER = SOURCES_TABLE_HEADER

# History usa a mesma estrutura de Sources
SourceHistoryEntry = SourceEntry


# ============================================================================
# SOURCES DLQ
# ============================================================================

SOURCES_DLQ_TABLE_NAME = "Sources DLQ"
SOURCES_DLQ_TABLE_HEADER = SOURCES_TABLE_HEADER + ["Mensagem de Erro"]


@dataclass
class SourceDLQEntry(SourceEntry):
    """
    Fonte que falhou durante processamento (Dead Letter Queue).

    Herda todos os campos de SourceEntry e adiciona mensagem de erro.
    Usada para rastreabilidade e debugging de falhas.

    Attributes:
        error_message (str): Descrição do erro que causou a falha.
    """

    error_message: str = ""

    def to_row(self) -> list[str]:
        """
        Converte para lista incluindo mensagem de erro.

        Returns:
            list[str]: Lista de valores com erro no final.
        """
        return super().to_row() + [self.error_message]
