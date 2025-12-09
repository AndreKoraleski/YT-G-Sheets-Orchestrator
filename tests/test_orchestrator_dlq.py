"""Tests for Orchestrator error handling and DLQ functionality."""

from unittest.mock import MagicMock, patch

import pytest

from orc.config import Config
from orc.orchestrator import Orchestrator


@pytest.fixture
def mock_config():
    """Fixture para Config mockado."""
    config = MagicMock(spec=Config)
    config.worker_name = "test-worker"
    config.spreadsheet_id = "test-sheet"
    return config


@pytest.fixture
def mock_orchestrator(mock_config):
    """Fixture para Orchestrator mockado."""
    with (
        patch("orc.orchestrator.get_spreadsheet"),
        patch("orc.orchestrator.WorkerTable") as mock_worker_table,
        patch("orc.orchestrator.TaskTable") as mock_task_table,
        patch("orc.orchestrator.SourceTable") as mock_source_table,
    ):
        orchestrator = Orchestrator(mock_config)
        orchestrator.worker_table = mock_worker_table.return_value
        orchestrator.task_table = mock_task_table.return_value
        orchestrator.source_table = mock_source_table.return_value
        yield orchestrator


class TestErrorHandling:
    """Testes para verificar que erros são capturados e enviados à DLQ."""

    def test_callback_exception_moves_task_to_dlq(self, mock_orchestrator):
        """
        TESTE CRÍTICO: Verifica que exceções no callback são capturadas
        e a task é movida para DLQ com a mensagem de erro.
        """
        # Arrange: Configura task disponível
        mock_task = MagicMock()
        mock_task.task_id = "task-123"
        mock_task.url = "https://youtube.com/watch?v=test"

        mock_orchestrator.task_table.claim_next_task.return_value = (
            1,  # row_number
            mock_task,
        )

        # Callback que levanta exceção
        def failing_callback(url: str) -> None:
            raise ValueError("Erro de teste: URL inválida")

        # Act: Processa task que falhará
        result = mock_orchestrator.process_next_task(failing_callback)

        # Assert: Verifica que move_to_dlq foi chamado com a mensagem de erro
        assert result is True  # Ainda retorna True (processado)
        mock_orchestrator.task_table.move_to_dlq.assert_called_once()

        # Verifica os argumentos do move_to_dlq
        call_args = mock_orchestrator.task_table.move_to_dlq.call_args
        assert call_args[0][0] == 1  # row_number
        assert call_args[0][1] == mock_task  # task object
        assert "Erro de teste: URL inválida" in call_args[0][2]  # error message

    def test_callback_runtime_error_captured(self, mock_orchestrator):
        """Verifica que RuntimeErrors são capturados e registrados."""
        # Arrange
        mock_task = MagicMock()
        mock_task.task_id = "task-456"
        mock_orchestrator.task_table.claim_next_task.return_value = (2, mock_task)

        def callback_with_runtime_error(url: str) -> None:
            raise RuntimeError("Erro de processamento")

        # Act
        mock_orchestrator.process_next_task(callback_with_runtime_error)

        # Assert
        mock_orchestrator.task_table.move_to_dlq.assert_called_once()
        error_message = mock_orchestrator.task_table.move_to_dlq.call_args[0][2]
        assert "Erro de processamento" in error_message

    def test_callback_success_moves_to_history(self, mock_orchestrator):
        """Verifica que callbacks bem-sucedidos movem task para History."""
        # Arrange
        mock_task = MagicMock()
        mock_orchestrator.task_table.claim_next_task.return_value = (3, mock_task)

        def successful_callback(url: str) -> None:
            print(f"Processando {url}")  # Sucesso

        # Act
        result = mock_orchestrator.process_next_task(successful_callback)

        # Assert
        assert result is True
        mock_orchestrator.task_table.move_to_history.assert_called_once_with(
            3, mock_task
        )
        mock_orchestrator.task_table.move_to_dlq.assert_not_called()

    def test_multiple_errors_all_captured(self, mock_orchestrator):
        """Verifica que múltiplos erros são capturados separadamente."""
        # Arrange
        tasks = [
            (1, MagicMock(task_id="task-1")),
            (2, MagicMock(task_id="task-2")),
            (3, MagicMock(task_id="task-3")),
        ]
        mock_orchestrator.task_table.claim_next_task.side_effect = tasks

        error_count = 0

        def callback_with_errors(url: str) -> None:
            nonlocal error_count
            error_count += 1
            raise ValueError(f"Erro número {error_count}")

        # Act
        for _ in range(3):
            mock_orchestrator.process_next_task(callback_with_errors)

        # Assert
        assert mock_orchestrator.task_table.move_to_dlq.call_count == 3
        calls = mock_orchestrator.task_table.move_to_dlq.call_args_list

        # Verifica que cada erro tem mensagem única
        assert "Erro número 1" in calls[0][0][2]
        assert "Erro número 2" in calls[1][0][2]
        assert "Erro número 3" in calls[2][0][2]




class TestDLQDocumentation:
    """Testes que servem como documentação do comportamento da DLQ."""

    def test_dlq_captures_str_of_exception(self, mock_orchestrator):
        """
        Demonstra que a DLQ captura str(e) da exceção.

        Isso significa que a mensagem de erro que aparece na planilha
        é exatamente o que você passa ao levantar a exceção.
        """
        mock_orchestrator.task_table.claim_next_task.return_value = (
            1,
            MagicMock(task_id="doc-task"),
        )

        def callback(url: str) -> None:
            # Esta mensagem aparecerá na DLQ
            raise ValueError("URL inválida: formato não suportado")

        mock_orchestrator.process_next_task(callback)

        # A mensagem na DLQ será exatamente esta
        error_in_dlq = mock_orchestrator.task_table.move_to_dlq.call_args[0][2]
        assert error_in_dlq == "URL inválida: formato não suportado"

    def test_dont_catch_exceptions_in_callback(self, mock_orchestrator):
        """
        ANTI-PATTERN: Não capture exceções dentro do callback.

        Se você capturar exceções, elas não chegarão à DLQ!
        """
        mock_orchestrator.task_table.claim_next_task.return_value = (
            1,
            MagicMock(task_id="bad-task"),
        )

        def bad_callback(url: str) -> None:
            try:
                # Este erro será escondido
                raise ValueError("Erro importante!")
            except ValueError:
                print("Erro capturado, mas perdido!")
                # Não propaga - DLQ não recebe nada!

        mock_orchestrator.process_next_task(bad_callback)

        # Task vai para History, não DLQ (ruim!)
        mock_orchestrator.task_table.move_to_history.assert_called_once()
        mock_orchestrator.task_table.move_to_dlq.assert_not_called()

    def test_good_error_handling_pattern(self, mock_orchestrator):
        """
        PADRÃO CORRETO: Propague exceções, deixe o orchestrator capturar.
        """
        mock_orchestrator.task_table.claim_next_task.return_value = (
            1,
            MagicMock(task_id="good-task"),
        )

        def good_callback(url: str) -> None:
            # Validação
            if not url.startswith("https://"):
                # Propaga - orchestrator capturará!
                raise ValueError(f"URL inválida: {url}")

            # Processamento
            result = process_video(url)

            # Se process_video levantar exceção, propaga também
            if not result.success:
                raise RuntimeError(result.error_message)

        def process_video(url):
            """Função auxiliar que pode falhar."""
            raise RuntimeError("Falha no download")

        mock_orchestrator.process_next_task(good_callback)

        # Task vai corretamente para DLQ
        mock_orchestrator.task_table.move_to_dlq.assert_called_once()
        error_msg = mock_orchestrator.task_table.move_to_dlq.call_args[0][2]
        assert "Falha no download" in error_msg
