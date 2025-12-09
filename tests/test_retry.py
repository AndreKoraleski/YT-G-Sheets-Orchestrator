"""Testes unitários para o módulo _retry."""
import pytest
import time
from unittest.mock import Mock
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound

from orc.gateway._retry import retry


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
        mock_func = Mock(side_effect=[
            Exception("erro 1"),
            Exception("erro 2"),
            "success"
        ])
        
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
        mock_func = Mock(side_effect=[
            Exception("erro 1"),
            Exception("erro 2"),
            "success"
        ])
        
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
