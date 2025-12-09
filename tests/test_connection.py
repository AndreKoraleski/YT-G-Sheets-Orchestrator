"""Testes unitários para o módulo connection."""

from unittest.mock import Mock, patch

import pytest
from gspread.exceptions import APIError, SpreadsheetNotFound

from orc.gateway.connection import get_spreadsheet


class TestGetSpreadsheet:
    """Testes para get_spreadsheet."""

    @patch("orc.gateway.connection._connect_service_account")
    def test_get_spreadsheet_success(self, mock_connect):
        """Deve obter spreadsheet com sucesso."""
        mock_client = Mock()
        mock_spreadsheet = Mock()
        mock_spreadsheet.title = "TestSpreadsheet"
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_connect.return_value = mock_client

        result = get_spreadsheet("test_sheet_id", "test_credentials.json")

        assert result == mock_spreadsheet
        mock_client.open_by_key.assert_called_once_with("test_sheet_id")

    @patch("orc.gateway.connection._connect_service_account")
    def test_get_spreadsheet_not_found(self, mock_connect):
        """Deve lançar SpreadsheetNotFound se planilha não existir."""
        mock_client = Mock()
        mock_client.open_by_key.side_effect = SpreadsheetNotFound("Spreadsheet not found")
        mock_connect.return_value = mock_client

        with pytest.raises(SpreadsheetNotFound):
            get_spreadsheet("invalid_sheet_id", "test_credentials.json")

    @patch("orc.gateway.connection._connect_service_account")
    def test_get_spreadsheet_api_error_retries(self, mock_connect):
        """Deve tentar retry em caso de erro de API."""
        mock_client = Mock()
        mock_spreadsheet = Mock()
        # Falha 2 vezes, sucesso na 3ª tentativa
        mock_client.open_by_key.side_effect = [
            APIError(Mock(status_code=500)),
            APIError(Mock(status_code=503)),
            mock_spreadsheet,
        ]
        mock_connect.return_value = mock_client

        result = get_spreadsheet("test_sheet_id", "test_credentials.json")

        assert result == mock_spreadsheet
        assert mock_client.open_by_key.call_count == 3


class TestConnectServiceAccount:
    """Testes para _connect_service_account."""

    @patch("orc.gateway.connection.Credentials.from_service_account_file")
    @patch("orc.gateway.connection.Client")
    def test_connect_service_account_success(self, mock_client_class, mock_creds):
        """Deve conectar usando service account com sucesso."""
        from orc.gateway.connection import _connect_service_account

        mock_credentials = Mock()
        mock_creds.return_value = mock_credentials
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        result = _connect_service_account("test_creds.json")

        assert result == mock_client
        mock_creds.assert_called_once_with(
            "test_creds.json", scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        mock_client_class.assert_called_once_with(auth=mock_credentials)
