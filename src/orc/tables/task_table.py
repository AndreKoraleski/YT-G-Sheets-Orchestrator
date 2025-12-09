"""
Gerenciador das tabelas de Tasks no Google Sheets.

Responsável por criar e gerenciar as 3 tabelas do pipeline:
- Tasks: Fila de trabalho principal
- Tasks History: Tarefas completadas
- Tasks DLQ: Tarefas que falharam
"""
import logging
from typing import Optional

from gspread import Spreadsheet, Worksheet
from .task_schema import (
    TaskEntry,
    TaskDLQEntry,
    TASKS_TABLE_NAME,
    TASKS_TABLE_HEADER,
    TASKS_HISTORY_TABLE_NAME,
    TASKS_HISTORY_TABLE_HEADER,
    TASKS_DLQ_TABLE_NAME,
    TASKS_DLQ_TABLE_HEADER
)
from ..gateway import (
    get_worksheet,
    get_header_mapping,
    pop_first_row_by_columns,
    append_row,
    delete_row
)

logger = logging.getLogger(__name__)


class TaskTable:
    """
    Gerencia o ciclo de vida das tasks no Google Sheets.
    
    Fornece métodos para reivindicar tasks da fila principal e movê-las
    para History (sucesso) ou DLQ (falha).
    """
    
    def __init__(self, spreadsheet: Spreadsheet, worker_name: str):
        """
        Inicializa as 3 tabelas de tasks.
        
        Args:
            spreadsheet (Spreadsheet): Instância do spreadsheet do gspread.
            worker_name (str): Nome do worker para ownership/claim.
        """
        self.worker_name = worker_name
        
        # Tabela principal (fila)
        self.tasks_ws: Worksheet = get_worksheet(
            spreadsheet,
            TASKS_TABLE_NAME,
            TASKS_TABLE_HEADER,
            replace_header=False,
            create=True
        )
        self.tasks_mapping: dict[str, int] = get_header_mapping(self.tasks_ws)
        
        # Tabela de histórico (sucessos)
        self.history_ws: Worksheet = get_worksheet(
            spreadsheet,
            TASKS_HISTORY_TABLE_NAME,
            TASKS_HISTORY_TABLE_HEADER,
            replace_header=False,
            create=True
        )
        
        # Tabela DLQ (erros)
        self.dlq_ws: Worksheet = get_worksheet(
            spreadsheet,
            TASKS_DLQ_TABLE_NAME,
            TASKS_DLQ_TABLE_HEADER,
            replace_header=False,
            create=True
        )
        
        logger.info("Task Tables inicializadas (Tasks, History, DLQ)")
    
    def _task_already_processed(self, task_id: str) -> bool:
        """
        Verifica se uma task já foi processada (sucesso ou falha).
        
        Checa tanto no histórico quanto na DLQ.
        
        Args:
            task_id (str): ID da task a verificar.
            
        Returns:
            bool: True se já foi processada, False caso contrário.
        """
        from ..gateway import select_first_by_columns
        
        # Verifica no histórico (sucessos)
        history_mapping = get_header_mapping(self.history_ws)
        if select_first_by_columns(self.history_ws, history_mapping, {"ID": task_id}):
            return True
        
        # Verifica na DLQ (falhas)
        dlq_mapping = get_header_mapping(self.dlq_ws)
        if select_first_by_columns(self.dlq_ws, dlq_mapping, {"ID": task_id}):
            return True
        
        return False
    
    def claim_next_task(self) -> Optional[tuple[int, TaskEntry]]:
        """
        Reivindica a próxima task disponível na fila principal.
        
        Usa pop_first_row_by_columns com claim column para garantir
        ownership exclusivo da task. Se a task já existe no histórico,
        remove-a silenciosamente e tenta a próxima.
        
        Loop infinito até encontrar task válida ou fila estar vazia.
        
        Returns:
            Optional[tuple[int, TaskEntry]]: Tupla (row_number, task) se encontrou,
                None se fila está vazia.
        """
        claim_column = "Worker Atribuído"
        
        while True:
            result = pop_first_row_by_columns(
                self.tasks_ws,
                self.tasks_mapping,
                {},  # Sem filtros - pega primeira disponível
                claim_column,
                self.worker_name
            )
            
            # Fila vazia - retorna None
            if not result:
                return None
            
            row_number, row_data = result
            task = TaskEntry.from_row(row_data)
            
            # Verifica se já foi processada (histórico ou DLQ)
            if self._task_already_processed(task.task_id):
                logger.warning(
                    f"Task duplicada encontrada: {task.task_id} (linha {row_number}). "
                    f"Já processada anteriormente. Removendo sem processar."
                )
                
                # Deleta a duplicata da fila
                claim_column_index = self.tasks_mapping.get("Worker Atribuído")
                delete_row(
                    self.tasks_ws,
                    row_number,
                    claim_column_index=claim_column_index,
                    claim_value=self.worker_name
                )
                
                # Continua loop para próxima task
                continue
            
            # Task válida encontrada - retorna
            task.claim(self.worker_name)
            logger.info(f"Task reivindicada: {task.task_id} (linha {row_number})")
            return (row_number, task)
    
    def move_to_history(self, row_number: int, task: TaskEntry) -> None:
        """
        Move uma task completada para a tabela History.
        
        Adiciona na History e remove da Tasks usando ownership verification.
        
        Args:
            row_number (int): Número da linha no Sheets.
            task (TaskEntry): Task completada a ser movida.
        """
        # Marca como completada se ainda não estiver
        if task.status != "COMPLETED":
            task.complete()
        
        # Append na History
        append_row(self.history_ws, task.to_row())
        
        # Delete da Tasks com verificação de ownership
        claim_column_index = self.tasks_mapping.get("Worker Atribuído")
        delete_row(
            self.tasks_ws,
            row_number,
            claim_column_index=claim_column_index,
            claim_value=self.worker_name
        )
        
        logger.info(f"Task movida para History: {task.task_id}")
    
    def move_to_dlq(self, row_number: int, task: TaskEntry, error_message: str) -> None:
        """
        Move uma task falhada para a tabela DLQ.
        
        Adiciona na DLQ com mensagem de erro e remove da Tasks usando
        ownership verification.
        
        Args:
            row_number (int): Número da linha no Sheets.
            task (TaskEntry): Task que falhou.
            error_message (str): Descrição do erro ocorrido.
        """
        # Marca como falhada se ainda não estiver
        if task.status != "FAILED":
            task.fail()
        
        # Cria entry DLQ com erro
        dlq_entry = TaskDLQEntry(
            task_id=task.task_id,
            source_id=task.source_id,
            url=task.url,
            name=task.name,
            duration=task.duration,
            created_at=task.created_at,
            claimed_at=task.claimed_at,
            completed_at=task.completed_at,
            status=task.status,
            assigned_worker=task.assigned_worker,
            error_message=error_message
        )
        
        # Append na DLQ
        append_row(self.dlq_ws, dlq_entry.to_row())
        
        # Delete da Tasks com verificação de ownership
        claim_column_index = self.tasks_mapping.get("Worker Atribuído")
        delete_row(
            self.tasks_ws,
            row_number,
            claim_column_index=claim_column_index,
            claim_value=self.worker_name
        )
        
        logger.info(f"Task movida para DLQ: {task.task_id} - {error_message[:50]}")
