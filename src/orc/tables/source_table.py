"""
Gerenciador das tabelas de Sources no Google Sheets.

Responsável por criar e gerenciar as 3 tabelas do pipeline:
- Sources: Fila de fontes (requer eleição de líder)
- Sources History: Fontes processadas com sucesso
- Sources DLQ: Fontes que falharam
"""
import logging
from typing import Optional

from gspread import Spreadsheet, Worksheet
from .source_schema import (
    SourceEntry,
    SourceDLQEntry,
    SOURCES_TABLE_NAME,
    SOURCES_TABLE_HEADER,
    SOURCES_HISTORY_TABLE_NAME,
    SOURCES_HISTORY_TABLE_HEADER,
    SOURCES_DLQ_TABLE_NAME,
    SOURCES_DLQ_TABLE_HEADER
)
from ..gateway import (
    get_worksheet,
    get_header_mapping,
    pop_first_row_by_columns,
    append_row,
    delete_row,
    update_row
)

logger = logging.getLogger(__name__)


class SourceTable:
    """
    Gerencia o ciclo de vida das fontes no Google Sheets.
    
    Requer liderança para reivindicar fontes. Ao reivindicar, extrai
    metadados via yt-dlp automaticamente.
    """
    
    def __init__(self, spreadsheet: Spreadsheet, worker_name: str):
        """
        Inicializa as 3 tabelas de sources.
        
        Args:
            spreadsheet (Spreadsheet): Instância do spreadsheet do gspread.
            worker_name (str): Nome do worker para ownership/claim.
        """
        self.worker_name = worker_name
        
        # Tabela principal (fila)
        self.sources_ws: Worksheet = get_worksheet(
            spreadsheet,
            SOURCES_TABLE_NAME,
            SOURCES_TABLE_HEADER,
            replace_header=False,
            create=True
        )
        self.sources_mapping: dict[str, int] = get_header_mapping(self.sources_ws)
        
        # Tabela de histórico (sucessos)
        self.history_ws: Worksheet = get_worksheet(
            spreadsheet,
            SOURCES_HISTORY_TABLE_NAME,
            SOURCES_HISTORY_TABLE_HEADER,
            replace_header=False,
            create=True
        )
        
        # Tabela DLQ (erros)
        self.dlq_ws: Worksheet = get_worksheet(
            spreadsheet,
            SOURCES_DLQ_TABLE_NAME,
            SOURCES_DLQ_TABLE_HEADER,
            replace_header=False,
            create=True
        )
        
        logger.info("Source Tables inicializadas (Sources, History, DLQ)")
    
    def add_source(self, url: str) -> SourceEntry:
        """
        Adiciona uma nova fonte à fila usando apenas a URL.
        
        Args:
            url (str): URL da fonte a ser processada.
            
        Returns:
            SourceEntry: Instância criada e adicionada.
        """
        source = SourceEntry.from_url(url)
        append_row(self.sources_ws, source.to_row())
        
        logger.info(f"Fonte adicionada: {source.source_id} - {url}")
        return source
    
    def _source_already_processed(self, source_id: str) -> bool:
        """
        Verifica se uma fonte já foi processada (sucesso ou falha).
        
        Checa tanto no histórico quanto na DLQ.
        
        Args:
            source_id (str): ID da fonte a verificar.
            
        Returns:
            bool: True se já foi processada, False caso contrário.
        """
        from ..gateway import select_first_by_columns
        
        # Verifica no histórico (sucessos)
        history_mapping = get_header_mapping(self.history_ws)
        if select_first_by_columns(self.history_ws, history_mapping, {"ID": source_id}):
            return True
        
        # Verifica na DLQ (falhas)
        dlq_mapping = get_header_mapping(self.dlq_ws)
        if select_first_by_columns(self.dlq_ws, dlq_mapping, {"ID": source_id}):
            return True
        
        return False
    
    def _extract_metadata_with_ytdlp(self, source: SourceEntry) -> bool:
        """
        Extrai metadados da fonte usando yt-dlp.
        
        Args:
            source (SourceEntry): Fonte a ter metadados extraídos.
            
        Returns:
            bool: True se extração bem-sucedida, False caso contrário.
        """
        try:
            import yt_dlp
            
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,  # Não baixa, só extrai info
                'skip_download': True
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(source.url, download=False)
                source.update_from_ytdlp(info_dict)
                
                logger.info(
                    f"Metadados extraídos: {source.source_id} - "
                    f"{source.name} ({source.video_count} vídeos)"
                )
                return True
                
        except Exception as e:
            logger.error(f"Erro ao extrair metadados de {source.source_id}: {e}")
            return False
    
    def claim_next_source(self) -> Optional[tuple[int, SourceEntry]]:
        """
        Reivindica a próxima fonte disponível e extrai metadados via yt-dlp.
        
        REQUER ELEIÇÃO DE LÍDER - Deve ser chamado apenas se worker é líder.
        
        Loop infinito até encontrar fonte válida ou fila estar vazia.
        Remove silenciosamente fontes duplicadas (já processadas).
        
        Returns:
            Optional[tuple[int, SourceEntry]]: Tupla (row_number, source) se encontrou,
                None se fila está vazia.
        """
        claim_column = "Worker Atribuído"
        
        while True:
            result = pop_first_row_by_columns(
                self.sources_ws,
                self.sources_mapping,
                {},  # Sem filtros - pega primeira disponível
                claim_column,
                self.worker_name
            )
            
            # Fila vazia - retorna None
            if not result:
                return None
            
            row_number, row_data = result
            source = SourceEntry.from_row(row_data)
            
            # Verifica se já foi processada (histórico ou DLQ)
            if self._source_already_processed(source.source_id):
                logger.warning(
                    f"Fonte duplicada encontrada: {source.source_id} (linha {row_number}). "
                    f"Já processada anteriormente. Removendo sem processar."
                )
                
                # Deleta a duplicata da fila
                claim_column_index = self.sources_mapping.get("Worker Atribuído")
                delete_row(
                    self.sources_ws,
                    row_number,
                    claim_column_index=claim_column_index,
                    claim_value=self.worker_name
                )
                
                # Continua loop para próxima fonte
                continue
            
            # Fonte válida encontrada - reivindica
            source.claim(self.worker_name)
            
            # Extrai metadados via yt-dlp
            extraction_success = self._extract_metadata_with_ytdlp(source)
            
            if not extraction_success:
                # Falha na extração - move para DLQ imediatamente
                logger.error(f"Falha ao extrair metadados da fonte {source.source_id}")
                self.move_to_dlq(row_number, source, "Falha na extração de metadados via yt-dlp")
                # Continua loop para próxima fonte
                continue
            
            # Atualiza linha no Sheets com metadados extraídos
            update_row(self.sources_ws, row_number, source.to_row())
            
            logger.info(f"Fonte reivindicada e metadados extraídos: {source.source_id} (linha {row_number})")
            return (row_number, source)
    
    def move_to_history(self, row_number: int, source: SourceEntry) -> None:
        """
        Move uma fonte completada para a tabela History.
        
        Args:
            row_number (int): Número da linha no Sheets.
            source (SourceEntry): Fonte completada a ser movida.
        """
        # Marca como completada se ainda não estiver
        if source.status != "COMPLETED":
            source.complete()
        
        # Append na History
        append_row(self.history_ws, source.to_row())
        
        # Delete da Sources com verificação de ownership
        claim_column_index = self.sources_mapping.get("Worker Atribuído")
        delete_row(
            self.sources_ws,
            row_number,
            claim_column_index=claim_column_index,
            claim_value=self.worker_name
        )
        
        logger.info(f"Fonte movida para History: {source.source_id}")
    
    def move_to_dlq(self, row_number: int, source: SourceEntry, error_message: str) -> None:
        """
        Move uma fonte falhada para a tabela DLQ.
        
        Args:
            row_number (int): Número da linha no Sheets.
            source (SourceEntry): Fonte que falhou.
            error_message (str): Descrição do erro ocorrido.
        """
        # Marca como falhada se ainda não estiver
        if source.status != "FAILED":
            source.fail()
        
        # Cria entry DLQ com erro
        dlq_entry = SourceDLQEntry(
            url=source.url,
            source_id=source.source_id,
            name=source.name,
            video_count=source.video_count,
            claimed_at=source.claimed_at,
            completed_at=source.completed_at,
            status=source.status,
            assigned_worker=source.assigned_worker,
            error_message=error_message
        )
        
        # Append na DLQ
        append_row(self.dlq_ws, dlq_entry.to_row())
        
        # Delete da Sources com verificação de ownership
        claim_column_index = self.sources_mapping.get("Worker Atribuído")
        delete_row(
            self.sources_ws,
            row_number,
            claim_column_index=claim_column_index,
            claim_value=self.worker_name
        )
        
        logger.info(f"Fonte movida para DLQ: {source.source_id} - {error_message[:50]}")
