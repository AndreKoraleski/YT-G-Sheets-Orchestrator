"""
Definição de schemas para Tasks (tarefas de processamento).

Define estruturas de dados para as 3 tabelas do pipeline:
- Tasks: Fila de trabalho principal
- Tasks History: Registro de tarefas completadas com sucesso
- Tasks DLQ: Dead Letter Queue para tarefas que falharam
"""
from datetime import datetime
from dataclasses import dataclass, field

# ============================================================================
# TASKS
# ============================================================================

TASKS_TABLE_NAME = "Tasks"
TASKS_TABLE_HEADER = [
    "ID",
    "ID da Fonte",
    "URL",
    "Nome",
    "Duração",
    "Timestamp de Criação",
    "Timestamp de Reivindicação",
    "Timestamp de Conclusão",
    "Status",
    "Worker Atribuído"
]


@dataclass
class TaskEntry:
    """
    Estrutura de dados para uma tarefa na fila de processamento.
    
    Representa uma unidade de trabalho (ex: vídeo) a ser processada por um worker.
    Armazena metadados, timestamps de lifecycle e ownership via claim column.
    
    Attributes:
        task_id (str): Identificador único da tarefa.
        source_id (str): ID da fonte que gerou esta tarefa.
        url (str): URL do recurso a ser processado.
        name (str): Título ou nome descritivo.
        duration (str): Duração em segundos.
        created_at (str): Timestamp de criação (ISO 8601).
        claimed_at (str): Timestamp quando worker reivindicou (ISO 8601).
        completed_at (str): Timestamp de finalização (ISO 8601).
        status (str): Estado atual (PENDING, CLAIMED, COMPLETED, FAILED).
        assigned_worker (str): Nome do worker que reivindicou (claim column).
    """
    task_id: str
    source_id: str
    url: str
    name: str
    duration: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    claimed_at: str = ""
    completed_at: str = ""
    status: str = "PENDING"
    assigned_worker: str = ""
    
    def claim(self, worker_name: str) -> None:
        """
        Reivindica a tarefa para um worker específico.
        
        Args:
            worker_name (str): Nome do worker que está reivindicando a tarefa.
        """
        self.assigned_worker = worker_name
        self.claimed_at = datetime.now().isoformat()
        self.status = "CLAIMED"
    
    def complete(self) -> None:
        """
        Marca a tarefa como completada com sucesso.
        """
        self.completed_at = datetime.now().isoformat()
        self.status = "COMPLETED"
    
    def fail(self) -> None:
        """
        Marca a tarefa como falhada.
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
            self.task_id,
            self.source_id,
            self.url,
            self.name,
            self.duration,
            self.created_at,
            self.claimed_at,
            self.completed_at,
            self.status,
            self.assigned_worker
        ]
    
    @classmethod
    def from_row(cls, row_data: list[str]) -> "TaskEntry":
        """
        Reconstrói instância a partir de dados do Sheets.
        
        Args:
            row_data (list[str]): Lista de valores de uma linha do Google Sheets.
            
        Returns:
            TaskEntry: Instância reconstruída.
        """
        return cls(
            task_id=row_data[0] if len(row_data) > 0 else "",
            source_id=row_data[1] if len(row_data) > 1 else "",
            url=row_data[2] if len(row_data) > 2 else "",
            name=row_data[3] if len(row_data) > 3 else "",
            duration=row_data[4] if len(row_data) > 4 else "",
            created_at=row_data[5] if len(row_data) > 5 else datetime.now().isoformat(),
            claimed_at=row_data[6] if len(row_data) > 6 else "",
            completed_at=row_data[7] if len(row_data) > 7 else "",
            status=row_data[8] if len(row_data) > 8 else "PENDING",
            assigned_worker=row_data[9] if len(row_data) > 9 else ""
        )


# ============================================================================
# TASKS HISTORY
# ============================================================================

TASKS_HISTORY_TABLE_NAME = "Tasks History"
TASKS_HISTORY_TABLE_HEADER = TASKS_TABLE_HEADER

# History usa a mesma estrutura de Tasks
TaskHistoryEntry = TaskEntry


# ============================================================================
# TASKS DLQ
# ============================================================================

TASKS_DLQ_TABLE_NAME = "Tasks DLQ"
TASKS_DLQ_TABLE_HEADER = TASKS_TABLE_HEADER + ["Mensagem de Erro"]


@dataclass
class TaskDLQEntry(TaskEntry):
    """
    Tarefa que falhou durante processamento (Dead Letter Queue).
    
    Herda todos os campos de TaskEntry e adiciona mensagem de erro.
    Usada para rastreabilidade e debugging de falhas.
    
    Attributes:
        error_message: Descrição do erro que causou a falha.
    """
    error_message: str = ""
    
    def to_row(self) -> list[str]:
        """
        Converte para lista incluindo mensagem de erro.
        
        Returns:
            list[str]: Lista de valores com erro no final.
        """
        return super().to_row() + [self.error_message]
    
    @classmethod
    def from_task(cls, task: TaskEntry, error_message: str) -> "TaskDLQEntry":
        """
        Cria DLQ entry a partir de TaskEntry falhada.
        
        Args:
            task: TaskEntry que falhou
            error_message: Mensagem de erro
            
        Returns:
            TaskDLQEntry
        """
        return cls(
            task_id=task.task_id,
            source_id=task.source_id,
            url=task.url,
            name=task.name,
            duration=task.duration,
            created_at=task.created_at,
            claimed_at=task.claimed_at,
            completed_at=task.completed_at or datetime.now().isoformat(),
            status="FAILED",
            assigned_worker=task.assigned_worker,
            error_message=error_message
        )
    
    @classmethod
    def from_row(cls, row_data: list[str]) -> "TaskDLQEntry":
        """Deserializa incluindo campo de erro."""
        base = super().from_row(row_data)
        base.error_message = row_data[10] if len(row_data) > 10 else ""
        return base
