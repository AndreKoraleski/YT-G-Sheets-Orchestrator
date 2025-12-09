import logging

from gspread import Spreadsheet, Worksheet

from ..gateway import (
    append_row,
    get_header_mapping,
    get_worksheet,
    select_first_by_columns,
    update_row,
)
from .worker_schema import WORKER_TABLE_HEADER, WORKER_TABLE_NAME, WorkerEntry

logger = logging.getLogger(__name__)


class WorkerTable:
    """
    Gerencia a tabela de Workers no Google Sheets com Write-Through Cache.
    """

    def __init__(self, spreadsheet: Spreadsheet, worker_name: str):
        """
        Inicializa a Worker Table e registra/recupera o worker.

        Args:
            spreadsheet: Instância do Spreadsheet do gspread
            worker_name: Nome do worker a ser registrado
        """
        self.worksheet: Worksheet = get_worksheet(
            spreadsheet, WORKER_TABLE_NAME, WORKER_TABLE_HEADER, replace_header=False, create=True
        )
        self.mapping: dict[str, int] = get_header_mapping(self.worksheet)
        self.worker_data: WorkerEntry = WorkerEntry(worker_name=worker_name)
        self.row_number: int | None = None

        self._register_worker()

        logger.info(
            f"Worker Table inicializada: {self.worker_data.worker_name} "
            f"({self.worker_data.worker_id}) na linha {self.row_number}"
        )

    def _register_worker(self) -> None:
        """
        Realiza o handshake do worker:
        1. Busca o worker na planilha pelo UUID
        2. Se encontrar, recupera dados remotos
        3. Se não encontrar, cria novo registro
        4. Atualiza estado para ACTIVE e salva
        """
        logger.info("Sincronizando sessão do worker...")

        result = select_first_by_columns(
            self.worksheet, self.mapping, {"ID do Worker": self.worker_data.worker_id}
        )

        if result:
            # Worker já existe - recupera dados
            self.row_number, current_row_data = result
            logger.info(f"Sessão recuperada na linha {self.row_number}.")
            self._sync_from_remote(current_row_data)
        else:
            # Novo worker - cria registro
            logger.info("Criando nova sessão...")
            row_values = self._serialize(self.worker_data)
            append_row(self.worksheet, row_values)

            # Busca a linha recém-criada
            retry_result = select_first_by_columns(
                self.worksheet, self.mapping, {"ID do Worker": self.worker_data.worker_id}
            )
            if retry_result:
                self.row_number, _ = retry_result
            else:
                logger.error("Falha crítica: Não foi possível localizar a linha após inserção.")
                raise RuntimeError("Worker registration failed")

        # Atualiza estado inicial e salva
        self.worker_data.status = "ACTIVE"
        self.worker_data.update_heartbeat()
        self.save_state()

    def _sync_from_remote(self, row_data: list[str]) -> None:
        """
        Sincroniza dados remotos com o objeto em memória.
        Evita que reinicializações zerem estatísticas acumuladas.

        Args:
            row_data: Dados da linha recuperada do Sheets
        """
        try:
            idx_tasks = self.mapping.get("Tarefas Processadas")
            if idx_tasks is not None and idx_tasks < len(row_data):
                val = row_data[idx_tasks]
                if val and val.isdigit():
                    old_count = self.worker_data.processed_tasks
                    self.worker_data.processed_tasks = int(val)
                    if old_count != self.worker_data.processed_tasks:
                        logger.info(
                            f"Contador de tarefas sincronizado: "
                            f"{old_count} -> {self.worker_data.processed_tasks}"
                        )

            idx_sources = self.mapping.get("Fontes Processadas")
            if idx_sources is not None and idx_sources < len(row_data):
                val = row_data[idx_sources]
                if val and val.isdigit():
                    old_count = self.worker_data.processed_sources
                    self.worker_data.processed_sources = int(val)
                    if old_count != self.worker_data.processed_sources:
                        logger.info(
                            f"Contador de fontes sincronizado: "
                            f"{old_count} -> {self.worker_data.processed_sources}"
                        )
        except Exception as e:
            logger.warning(f"Erro ao sincronizar dados remotos: {e}")

    def save_state(self) -> None:
        """
        Write-Through: Persiste o estado atual diretamente na linha cacheada.
        """
        if self.row_number is None:
            logger.warning("Tentativa de salvar estado sem uma linha vinculada.")
            return

        row_values = self._serialize(self.worker_data)
        update_row(self.worksheet, self.row_number, row_values)

        logger.debug(
            f"State saved (Line {self.row_number}): "
            f"Status={self.worker_data.status}, "
            f"Tasks={self.worker_data.processed_tasks}, "
            f"Sources={self.worker_data.processed_sources}"
        )

    def send_heartbeat(self) -> None:
        """
        Atualiza o timestamp do heartbeat e persiste o estado.
        """
        self.worker_data.update_heartbeat()
        self.save_state()

    def increment_tasks(self, count: int = 1) -> None:
        """
        Incrementa o contador de tarefas processadas e persiste o estado.

        Args:
            count: Número de tarefas a incrementar
        """
        self.worker_data.increment_tasks(count)
        self.save_state()

    def increment_sources(self, count: int = 1) -> None:
        """
        Incrementa o contador de fontes processadas e persiste o estado.

        Args:
            count: Número de fontes a incrementar
        """
        self.worker_data.increment_sources(count)
        self.save_state()

    def _serialize(self, worker: WorkerEntry) -> list[str]:
        """
        Serializa WorkerEntry para formato de linha do Sheets.

        Args:
            worker: Objeto WorkerEntry a ser serializado

        Returns:
            Lista de strings no formato do header
        """
        return [
            str(worker.worker_id),
            str(worker.worker_name),
            worker.last_heartbeat,
            str(worker.status),
            str(worker.processed_tasks),
            str(worker.processed_sources),
        ]
