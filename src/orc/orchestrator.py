import atexit
import hashlib
import logging
import re
import signal
from collections.abc import Callable

import yt_dlp
from gspread import Spreadsheet

from .config import Config
from .gateway import configure_rate_limiting, get_spreadsheet, update_active_workers
from .gateway.leader import release_leadership, try_acquire_leadership
from .tables.source_table import SourceTable
from .tables.task_schema import TaskEntry
from .tables.task_table import TaskTable
from .tables.worker_table import WorkerTable

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, config: Config | None = None):
        """
        Inicializa o orquestrador e estabelece a sessão do worker.
        """
        self.config: Config = config or Config()

        # Estado interno
        self.spreadsheet: Spreadsheet | None = None
        self.worker_table: WorkerTable | None = None
        self.task_table: TaskTable | None = None
        self.source_table: SourceTable | None = None

        # Inicialização
        self._setup_gateway()
        self._setup_worker_table()
        self._setup_task_table()
        self._setup_source_table()
        self._setup_shutdown_handlers()

    def _setup_gateway(self) -> None:
        """
        Estabelece a conexão com o Google Sheets e configura rate limiting.
        """
        logger.info("Conectando ao Google Sheets...")

        # Configura rate limiting global do gateway (valores padrão)
        configure_rate_limiting(rate_limit_seconds=1.0, jitter_max_seconds=0.5)

        self.spreadsheet = get_spreadsheet(
            self.config.spreadsheet_id, self.config.service_account_file
        )

    def _setup_worker_table(self) -> None:
        """
        Inicializa a Worker Table (cria/recupera dados do worker).
        """
        self.worker_table = WorkerTable(self.spreadsheet, self.config.worker_name)

        # Atualiza contagem inicial de workers ativos para rate limiting
        self._update_active_workers_count()

    def _setup_task_table(self) -> None:
        """
        Inicializa a Task Table.
        """
        self.task_table = TaskTable(self.spreadsheet, self.config.worker_name)

    def _setup_source_table(self) -> None:
        """
        Inicializa a Source Table.
        """
        self.source_table = SourceTable(self.spreadsheet, self.config.worker_name)

    def _setup_shutdown_handlers(self) -> None:
        """
        Configura handlers para shutdown gracioso.
        """
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        atexit.register(self._cleanup)
        logger.info("Shutdown handlers configurados")

    def _shutdown_handler(self, signum: int, frame) -> None:
        """
        Handler para sinais de shutdown (SIGINT/SIGTERM).

        Args:
            signum: Número do sinal recebido
            frame: Frame de execução atual
        """
        logger.warning(f"Sinal {signum} recebido, iniciando shutdown gracioso...")
        self._cleanup()
        exit(0)

    def _cleanup(self) -> None:
        """
        Cleanup ao finalizar: marca worker como INACTIVE e libera liderança.
        """
        try:
            logger.info("Iniciando cleanup do worker...")

            # Marca worker como inativo
            self.worker_table.worker_data.status = "INACTIVE"
            self.worker_table.save_state()
            logger.info(f"Worker {self.config.worker_name} marcado como INACTIVE")

            # Libera liderança se estiver ativa
            from .gateway import release_leadership

            release_leadership(self.spreadsheet, "Source", self.worker_table.worker_data.worker_id)
            logger.info("Liderança liberada")

        except Exception as e:
            logger.error(f"Erro durante cleanup: {e}", exc_info=True)

    def send_heartbeat(self) -> None:
        """
        Envia um sinal de vida para indicar que o worker está ativo.
        Também atualiza o contador de workers ativos para rate limiting dinâmico.
        """
        self.worker_table.send_heartbeat()
        self._update_active_workers_count()

    def _update_active_workers_count(self) -> None:
        """
        Conta workers ativos e atualiza rate limiting dinâmico.
        """
        try:
            from .gateway import get_column_values, get_header_mapping

            # Busca o índice da coluna Status
            header_mapping = get_header_mapping(self.worker_table.worksheet)
            status_column_index = header_mapping["Status"]

            # Busca coluna de Status (get_column_values usa 1-based index)
            status_values = get_column_values(self.worker_table.worksheet, status_column_index + 1)

            # Conta quantos estão ACTIVE (ignora o header)
            active_count = sum(1 for status in status_values[1:] if status == "ACTIVE")

            # Atualiza rate limiting global
            update_active_workers(active_count)

        except Exception as e:
            logger.warning(f"Erro ao atualizar contagem de workers ativos: {e}")

    def update_task_count(self, increment: int = 1) -> None:
        """
        Incrementa contador de tarefas processadas.

        Args:
            increment (int): Quantidade a incrementar.
        """
        self.worker_table.increment_tasks(increment)

    def _create_tasks_from_source(self, source_row: int, source) -> int:
        """
        Cria tasks a partir de uma source usando yt-dlp.

        Adiciona tasks em batches de 10 para evitar timeouts.

        Args:
            source_row (int): Número da linha da source no Sheets.
            source: SourceEntry processada com metadados.

        Returns:
            int: Número total de tasks criadas.
        """
        tasks_created = 0
        batch_size = 10
        batch = []

        try:
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "extract_flat": "in_playlist",
                "skip_download": True,
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(source.url, download=False)

                # Se for playlist/channel com múltiplos vídeos
                if "entries" in info_dict:
                    entries = list(info_dict["entries"])

                    for entry in entries:
                        if entry is None:
                            continue

                        # Cria task com ID extraído da URL do YouTube
                        video_url = entry.get("url") or entry.get("webpage_url", "")

                        # Extrai video ID da URL do YouTube
                        video_id_match = re.search(
                            r"(?:v=|\/)([a-zA-Z0-9_-]{11})(?:\?|&|$)", video_url
                        )
                        if video_id_match:
                            video_id = video_id_match.group(1)
                        else:
                            # Fallback: usa hash se não conseguir extrair
                            video_id = hashlib.md5(video_url.encode()).hexdigest()[:11]

                        task = TaskEntry(
                            task_id=video_id,
                            source_id=source.source_id,
                            url=video_url,
                            name=entry.get("title", "Unknown"),
                            duration=str(entry.get("duration", 0)),
                        )

                        batch.append(task.to_row())

                        # Adiciona batch quando atingir o tamanho
                        if len(batch) >= batch_size:
                            from .gateway import append_rows

                            append_rows(self.task_table.tasks_ws, batch)
                            tasks_created += len(batch)
                            logger.info(
                                f"Batch de {len(batch)} tasks adicionadas ({tasks_created} total)"
                            )
                            batch = []

                else:
                    # Vídeo único - extrai ID da URL
                    video_url = info_dict.get("webpage_url") or info_dict.get("url", "")

                    video_id_match = re.search(r"(?:v=|\/)([a-zA-Z0-9_-]{11})(?:\?|&|$)", video_url)
                    if video_id_match:
                        video_id = video_id_match.group(1)
                    else:
                        # Fallback: usa hash se não conseguir extrair
                        video_id = hashlib.md5(video_url.encode()).hexdigest()[:11]

                    task = TaskEntry(
                        task_id=video_id,
                        source_id=source.source_id,
                        url=video_url,
                        name=info_dict.get("title", "Unknown"),
                        duration=str(info_dict.get("duration", 0)),
                    )

                    batch.append(task.to_row())

                # Adiciona último batch (se houver)
                if batch:
                    from .gateway import append_rows

                    append_rows(self.task_table.tasks_ws, batch)
                    tasks_created += len(batch)
                    logger.info(
                        f"Batch final de {len(batch)} tasks adicionadas ({tasks_created} total)"
                    )

                # Move source para history após criar todas as tasks
                self.source_table.move_to_history(source_row, source)
                logger.info(f"Source {source.source_id} processada: {tasks_created} tasks criadas")

                return tasks_created

        except Exception as e:
            logger.error(f"Erro ao criar tasks da source {source.source_id}: {e}")
            self.source_table.move_to_dlq(source_row, source, str(e))
            return 0

    def process_next_task(self, callback: Callable[[str], None]) -> bool:
        """
        Método central: Processa a próxima task disponível.

        Fluxo:
        1. Tenta reivindicar uma task pendente
        2. Se não há tasks, tenta virar líder e criar tasks de uma source
        3. Se conseguiu task, chama callback com a URL
        4. Se não há mais trabalho, retorna False

        Args:
            callback (Callable[[str], None]): Função a ser chamada com a URL da task.

        Returns:
            bool: True se processou/criou algo, False se não há mais trabalho.
        """
        # Tenta reivindicar task existente
        result = self.task_table.claim_next_task()

        if result:
            row_number, task = result
            logger.info(f"Task reivindicada: {task.task_id}")

            # Chama callback com a URL
            try:
                callback(task.url)
                # Marca task como completa e move para history
                self.task_table.move_to_history(row_number, task)
                self.update_task_count()
                return True

            except Exception as e:
                logger.error(f"Erro ao processar task {task.task_id}: {e}")
                self.task_table.move_to_dlq(row_number, task, str(e))
                return True  # Processou (mesmo com erro)

        # Não há tasks - tenta virar líder para criar tasks de sources
        logger.info("Nenhuma task disponível. Tentando eleição para criar tasks...")

        is_leader = try_acquire_leadership(
            self.spreadsheet,
            "Source",
            self.worker_table.worker_data.worker_id,
            ttl_seconds=300,  # 5 minutos
        )

        if not is_leader:
            logger.info("Não conseguiu liderança. Nenhum trabalho disponível.")
            return False

        try:
            logger.info("Liderança adquirida. Processando source...")

            # Reivindica próxima source (já extrai metadados via yt-dlp)
            source_result = self.source_table.claim_next_source()

            if not source_result:
                logger.info("Nenhuma source disponível. Nenhum trabalho.")
                release_leadership(
                    self.spreadsheet, "Source", self.worker_table.worker_data.worker_id
                )
                return False

            source_row, source = source_result

            # Cria tasks da source (10 em 10)
            tasks_created = self._create_tasks_from_source(source_row, source)

            # Incrementa contador de sources processadas
            if tasks_created > 0:
                self.worker_table.increment_sources()

            release_leadership(self.spreadsheet, "Source", self.worker_table.worker_data.worker_id)

            if tasks_created > 0:
                logger.info(
                    f"Source processada: {tasks_created} tasks criadas. Processando próxima iteração..."
                )
                # Chama recursivamente para processar primeira task criada
                return self.process_next_task(callback)
            else:
                logger.warning("Nenhuma task criada da source.")
                return False

        except Exception as e:
            logger.error(f"Erro ao processar source: {e}")
            release_leadership(self.spreadsheet, "Source", self.worker_table.worker_data.worker_id)
            return False
