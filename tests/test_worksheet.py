"""
Testes unitários completos e consolidados para o módulo worksheet.
"""

from unittest.mock import Mock, patch

import pytest
from gspread.exceptions import WorksheetNotFound

from orc.gateway.worksheet import (
    _create_worksheet,
    _enforce_header,
    get_header_mapping,
    get_worksheet,
)


class TestGetHeaderMapping:
    """Testes para get_header_mapping."""

    def test_get_header_mapping_success(self):
        """Deve retornar mapeamento correto do header."""
        mock_worksheet = Mock()
        mock_worksheet.title = "TestSheet"
        mock_worksheet.row_values.return_value = ["ID", "Nome", "Status"]

        result = get_header_mapping(mock_worksheet)

        assert result == {"ID": 0, "Nome": 1, "Status": 2}

    def test_get_header_mapping_with_duplicates_raises_error(self):
        """Deve lançar ValueError se houver duplicatas no header."""
        mock_worksheet = Mock()
        mock_worksheet.title = "TestSheet"
        mock_worksheet.row_values.return_value = ["ID", "Nome", "ID"]

        with pytest.raises(ValueError, match="duplicado encontrado"):
            get_header_mapping(mock_worksheet)

    def test_get_header_mapping_api_error_retries(self):
        """Deve fazer retry em caso de erro de API."""
        mock_worksheet = Mock()
        mock_worksheet.title = "TestSheet"
        mock_worksheet.row_values.side_effect = [Exception("API Error"), ["ID", "Nome"]]

        result = get_header_mapping(mock_worksheet)

        assert result == {"ID": 0, "Nome": 1}
        assert mock_worksheet.row_values.call_count == 2


class TestCreateWorksheet:
    """Testes para _create_worksheet."""

    def test_create_worksheet_empty_header(self):
        """Deve criar worksheet sem header se lista estiver vazia."""
        mock_spreadsheet = Mock()
        mock_worksheet = Mock()
        mock_spreadsheet.add_worksheet.return_value = mock_worksheet

        result = _create_worksheet(mock_spreadsheet, "NewSheet", [])

        assert result == mock_worksheet
        mock_worksheet.append_row.assert_not_called()

    def test_create_worksheet_api_error_retries(self):
        """Deve fazer retry se add_worksheet falhar."""
        mock_spreadsheet = Mock()
        mock_worksheet = Mock()
        mock_spreadsheet.add_worksheet.side_effect = [Exception("API Error"), mock_worksheet]

        result = _create_worksheet(mock_spreadsheet, "NewSheet", ["Col1"])

        assert result == mock_worksheet
        assert mock_spreadsheet.add_worksheet.call_count == 2


class TestGetWorksheet:
    """Testes para get_worksheet."""

    def test_get_worksheet_exists_no_header(self):
        """Deve retornar worksheet existente sem validar header."""
        mock_worksheet = Mock()
        mock_worksheet.title = "ExistingSheet"
        mock_spreadsheet = Mock()
        mock_spreadsheet.title = "TestSpreadsheet"
        mock_spreadsheet.worksheet.return_value = mock_worksheet

        result = get_worksheet(
            mock_spreadsheet, "ExistingSheet", [], replace_header=False, create=False
        )

        assert result == mock_worksheet

    def test_get_worksheet_not_found_create_true(self):
        """Deve criar worksheet se não existir e create=True."""
        mock_new_worksheet = Mock()
        mock_new_worksheet.title = "NewSheet"
        mock_spreadsheet = Mock()
        mock_spreadsheet.title = "TestSpreadsheet"
        mock_spreadsheet.worksheet.side_effect = WorksheetNotFound("Not found")
        mock_spreadsheet.add_worksheet.return_value = mock_new_worksheet

        result = get_worksheet(
            mock_spreadsheet, "NewSheet", ["Col1", "Col2"], replace_header=False, create=True
        )

        assert result == mock_new_worksheet

    def test_get_worksheet_not_found_create_false(self):
        """Deve lançar exceção se não existir e create=False."""
        mock_spreadsheet = Mock()
        mock_spreadsheet.worksheet.side_effect = WorksheetNotFound("Not found")

        with pytest.raises(WorksheetNotFound):
            get_worksheet(mock_spreadsheet, "NonExistent", [], replace_header=False, create=False)

    @patch("orc.gateway.worksheet._create_worksheet")
    @patch("orc.gateway.worksheet.retry")
    def test_get_worksheet_creates_when_not_found(self, mock_retry, mock_create):
        """Deve criar worksheet quando WorksheetNotFound e create=True (linha 95)."""
        mock_retry.side_effect = WorksheetNotFound("Worksheet not found")

        mock_new_worksheet = Mock()
        mock_new_worksheet.title = "NewSheet"
        mock_create.return_value = mock_new_worksheet

        mock_spreadsheet = Mock()
        mock_spreadsheet.title = "TestSpreadsheet"

        result = get_worksheet(
            spreadsheet=mock_spreadsheet,
            worksheet_name="NewSheet",
            header=["Col1", "Col2"],
            replace_header=False,
            create=True,
        )

        mock_create.assert_called_once_with(mock_spreadsheet, "NewSheet", ["Col1", "Col2"])
        assert result == mock_new_worksheet


class TestEnforceHeader:
    """Testes para _enforce_header."""

    def test_enforce_header_replace_false_matching(self):
        """Não deve fazer nada se header já estiver correto."""
        mock_worksheet = Mock()
        mock_worksheet.title = "TestSheet"
        mock_worksheet.row_values.return_value = ["Col1", "Col2"]

        _enforce_header(mock_worksheet, ["Col1", "Col2"], replace_header=False)

        mock_worksheet.update.assert_not_called()

    def test_enforce_header_replace_false_not_matching(self):
        """Deve lançar ValueError se header não bater e replace_header=False."""
        mock_worksheet = Mock()
        mock_worksheet.title = "TestSheet"
        mock_worksheet.row_values.return_value = ["Col1", "Col2"]

        with pytest.raises(ValueError, match="não corresponde ao esperado"):
            _enforce_header(mock_worksheet, ["NewCol1", "NewCol2"], replace_header=False)
