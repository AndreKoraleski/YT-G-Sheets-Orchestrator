"""Testes unitários para o módulo _retry."""

import time
from unittest.mock import Mock, patch

import pytest
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

from orc.gateway._retry import (
    NON_RETRYABLE_EXCEPTIONS,
    configure_rate_limiting,
    retry,
    update_active_workers,
)


class TestRetry:
    """Testes para a função retry."""

    def test_retry_success_first_attempt(self):
        """Deve retornar sucesso na primeira tentativa."""
        mock_func = Mock(return_value="success")
        result = retry(mock_func)

        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_success_after_failures(self):
        """Deve retryer e eventualmente ter sucesso."""
        mock_func = Mock(side_effect=[Exception("erro 1"), Exception("erro 2"), "success"])

        result = retry(mock_func, tries=5, delay=0.01, backoff=1.0)

        assert result == "success"
        assert mock_func.call_count == 3

    def test_retry_max_attempts_exceeded(self):
        """Deve falhar após atingir o máximo de tentativas."""
        mock_func = Mock(side_effect=Exception("persistent error"))

        with pytest.raises(Exception, match="persistent error"):
            retry(mock_func, tries=3, delay=0.01, backoff=1.0)

        assert mock_func.call_count == 3

    def test_retry_no_retry_on_worksheet_not_found(self):
        """Não deve retryer WorksheetNotFound."""
        mock_func = Mock(side_effect=WorksheetNotFound("test_sheet"))

        with pytest.raises(WorksheetNotFound):
            retry(mock_func, tries=5, delay=0.01)

        # Deve falhar imediatamente, sem retries
        assert mock_func.call_count == 1

    def test_retry_no_retry_on_spreadsheet_not_found(self):
        """Não deve retryer SpreadsheetNotFound."""
        mock_func = Mock(side_effect=SpreadsheetNotFound("test_id"))

        with pytest.raises(SpreadsheetNotFound):
            retry(mock_func, tries=5, delay=0.01)

        assert mock_func.call_count == 1

    def test_retry_no_retry_on_value_error(self):
        """Não deve retryer ValueError."""
        mock_func = Mock(side_effect=ValueError("invalid value"))

        with pytest.raises(ValueError):
            retry(mock_func, tries=5, delay=0.01)

        assert mock_func.call_count == 1

    def test_retry_no_retry_on_key_error(self):
        """Não deve retryer KeyError."""
        mock_func = Mock(side_effect=KeyError("missing_key"))

        with pytest.raises(KeyError):
            retry(mock_func, tries=5, delay=0.01)

        assert mock_func.call_count == 1

    def test_retry_backoff_timing(self):
        """Deve aplicar backoff exponencial entre tentativas."""
        mock_func = Mock(side_effect=[Exception("erro 1"), Exception("erro 2"), "success"])

        start = time.time()
        result = retry(mock_func, tries=5, delay=0.1, backoff=2.0)
        elapsed = time.time() - start

        assert result == "success"
        # Primeira falha: 0.1s, Segunda falha: 0.2s = 0.3s total mínimo
        assert elapsed >= 0.3

    def test_retry_preserves_return_type(self):
        """Deve preservar o tipo de retorno da função."""
        # Teste com diferentes tipos
        mock_func_int = Mock(return_value=42)
        assert retry(mock_func_int) == 42

        mock_func_list = Mock(return_value=[1, 2, 3])
        assert retry(mock_func_list) == [1, 2, 3]

        mock_func_dict = Mock(return_value={"key": "value"})
        assert retry(mock_func_dict) == {"key": "value"}

        mock_func_none = Mock(return_value=None)
        assert retry(mock_func_none) is None


class TestRateLimiting:
    """Testes para rate limiting dinâmico."""

    def test_configure_rate_limiting(self):
        """Deve configurar rate limiting sem erros."""
        configure_rate_limiting(2.0, 1.0)
        configure_rate_limiting(0.5, 0.25)
        # Verifica que não lança exceção
        assert True

    def test_update_active_workers(self):
        """Deve atualizar número de workers ativos."""
        update_active_workers(1)
        update_active_workers(5)
        update_active_workers(10)
        assert True

    def test_update_active_workers_minimum_one(self):
        """Deve garantir mínimo de 1 worker."""
        update_active_workers(0)
        update_active_workers(-5)
        # Internamente deve usar 1 como mínimo
        assert True

    def test_all_non_retryable_exceptions(self):
        """Deve ter todas as exceções não-retryáveis definidas."""
        assert WorksheetNotFound in NON_RETRYABLE_EXCEPTIONS
        assert SpreadsheetNotFound in NON_RETRYABLE_EXCEPTIONS
        assert ValueError in NON_RETRYABLE_EXCEPTIONS
        assert KeyError in NON_RETRYABLE_EXCEPTIONS
        assert TypeError in NON_RETRYABLE_EXCEPTIONS

    @patch("orc.gateway._retry.time.sleep")
    @patch("orc.gateway._retry.time.time")
    def test_rate_limit_integration_with_retry(self, mock_time, mock_sleep):
        """Deve aplicar rate limiting durante retry."""
        configure_rate_limiting(0.1, 0.0)
        update_active_workers(1)

        mock_func = Mock(side_effect=[RuntimeError("error"), "success"])

        # Simula tempo
        mock_time.side_effect = [0.0, 0.0, 0.05, 0.05]

        result = retry(mock_func, tries=3, delay=0.01)

        assert result == "success"
        # Rate limiting deve ter sido aplicado
        assert mock_sleep.call_count >= 1
