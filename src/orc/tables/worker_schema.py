import json
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field


# Constantes para a tabela de workers
WORKER_TABLE_NAME = "Workers"
WORKER_TABLE_HEADER = [
    "ID do Worker",
    "Nome do Worker",
    "Último Heartbeat",
    "Status",
    "Tarefas Processadas",
    "Fontes Processadas"
]


@dataclass
class WorkerEntry:
    """
    Representa a identidade e o estado atual de um Worker em memória.
    Gerencia a persistência local do UUID para manter a identidade entre reinicializações.

    Attributes:
        worker_id (str): O UUID único do worker (somente leitura).
        worker_name (str): O nome do worker.
        last_heartbeat (str): Timestamp do último heartbeat enviado (ISO 8601).
        status (str): O status atual do worker (e.g., "ACTIVE", "INACTIVE").
        processed_tasks (int): Número de tarefas processadas pelo worker.
    """
    worker_name: str
    _worker_id: str = field(init=False)
    last_heartbeat: str = field(default_factory=lambda: datetime.now().isoformat())
    status: str = "ACTIVE"
    processed_tasks: int = 0
    processed_sources: int = 0

    def __post_init__(self):
        self._worker_id = self._get_or_create_uuid()

    @property
    def worker_id(self) -> str:
        """
        O UUID do worker (somente leitura).
        """
        return self._worker_id

    def update_heartbeat(self):
        """Atualiza o timestamp de heartbeat para o momento atual (ISO 8601)."""
        self.last_heartbeat = datetime.now().isoformat()

    def increment_tasks(self, count: int = 1):
        """Incrementa o contador de tarefas processadas."""
        self.processed_tasks += count

    def increment_sources(self, count: int = 1):
        """Incrementa o contador de fontes processadas."""
        self.processed_sources += count

    def _get_or_create_uuid(self) -> str:
        """Gerencia a persistência local do UUID."""
        path = Path(__file__).parent.parent / "data"
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / "worker_id.json"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)["worker_id"]
        except (FileNotFoundError, KeyError, json.JSONDecodeError):
            new_id = str(uuid.uuid4())
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump({"worker_id": new_id}, f)
            return new_id

    def to_row(self) -> list:
        """Converte WorkerEntry para lista de valores para o Google Sheets."""
        return [
            self.worker_id,
            self.worker_name,
            self.last_heartbeat,
            self.status,
            str(self.processed_tasks),
            str(self.processed_sources)
        ]