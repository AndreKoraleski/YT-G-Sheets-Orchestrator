"""
Testes unitários completos e consolidados para o módulo operations.
"""
from unittest.mock import Mock, patch
from orc.gateway.operations import (
    get_column_values,
    _verify_ownership,
    pop_first_row_by_columns,
    select_first_by_columns,
    select_all_by_columns,
    append_row,
    append_rows,
    get_row,
    get_rows,
    get_rows_by_numbers,
    get_next_valid_row,
    update_row,
    delete_row,
    move_row
)


# ============================================================================
# TESTES BÁSICOS - get_column_values e _verify_ownership
# ============================================================================

class TestGetColumnValues:
    """Testes para get_column_values."""
    
    def test_get_column_values_success(self):
        """Deve retornar valores da coluna com sucesso."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_values.return_value = ['Header', 'Value1', 'Value2', 'Value3']
        
        result = get_column_values(mock_worksheet, 1)
        
        assert result == ['Header', 'Value1', 'Value2', 'Value3']
        mock_worksheet.col_values.assert_called_once_with(1)
    
    def test_get_column_values_empty(self):
        """Deve retornar lista vazia se coluna estiver vazia."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_values.return_value = []
        
        result = get_column_values(mock_worksheet, 1)
        
        assert result == []
    
    def test_get_column_values_api_error_retries(self):
        """Deve fazer retry em caso de erro de API."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_values.side_effect = [
            Exception('API Error'),
            ['Header', 'Value1']
        ]
        
        result = get_column_values(mock_worksheet, 1)
        
        assert result == ['Header', 'Value1']
        assert mock_worksheet.col_values.call_count == 2


class TestVerifyOwnership:
    """Testes para _verify_ownership."""
    
    @patch('orc.gateway.operations.time.sleep')
    def test_verify_ownership_success(self, mock_sleep):
        """Deve retornar True se claim bater após espera."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_cell = Mock()
        mock_cell.value = 'worker1'
        mock_worksheet.cell.return_value = mock_cell
        
        result = _verify_ownership(mock_worksheet, 2, 2, 'worker1')
        
        assert result is True
        mock_sleep.assert_called_once_with(1.0)
    
    @patch('orc.gateway.operations.time.sleep')
    def test_verify_ownership_failed(self, mock_sleep):
        """Deve retornar False se claim não bater."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.row_values.return_value = ['id1', 'name1', 'worker2']
        
        result = _verify_ownership(mock_worksheet, 2, 2, 'worker1')
        
        assert result is False
    
    @patch('orc.gateway.operations.time.sleep')
    def test_verify_ownership_empty_claim(self, mock_sleep):
        """Deve retornar False se claim estiver vazio."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.row_values.return_value = ['id1', 'name1', '']
        
        result = _verify_ownership(mock_worksheet, 2, 2, 'worker1')
        
        assert result is False
    
    def test_verify_ownership_no_claim_column(self):
        """Deve retornar True se não houver coluna de claim."""
        mock_worksheet = Mock()
        
        result = _verify_ownership(mock_worksheet, 2, None, 'worker1')
        
        assert result is True
    
    @patch('orc.gateway.operations.time.sleep')
    def test_verify_ownership_exception(self, mock_sleep):
        """Deve retornar False se houver exceção."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.row_values.side_effect = Exception('API Error')
        
        result = _verify_ownership(mock_worksheet, 2, 2, 'worker1')
        
        assert result is False


# ============================================================================
# TESTES POP_FIRST_ROW - Incluindo linhas missing
# ============================================================================

class TestPopFirstRow:
    """Testes para pop_first_row_by_columns."""
    
    def test_pop_first_row_column_not_found(self):
        """Deve retornar None se coluna não existir no mapping."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        mapping = {'id': 0, 'name': 1}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'nonexistent_column',
            'worker1'
        )
        
        assert result is None
    
    def test_pop_first_row_no_rows_available(self):
        """Deve retornar None se não houver linhas disponíveis."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name', 'claim']
        ]
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert result is None
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_pop_first_row_update_cell_exception_continues(self, mock_verify):
        """Deve continuar para próximo attempt se update_cell falhar."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        call_count = [0]
        
        def get_all_values_side_effect():
            call_count[0] += 1
            return [
                ['id', 'name', 'claim'],
                ['1', 'Alice', '']
            ]
        
        mock_worksheet.get_all_values = Mock(side_effect=get_all_values_side_effect)
        mock_worksheet.update_cell = Mock(side_effect=[Exception('API Error'), None])
        mock_verify.return_value = True
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert mock_worksheet.update_cell.call_count == 2
        assert result is not None
    
    def test_pop_first_row_empty_rows_only(self):
        """Deve retornar None se todas linhas forem vazias."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name', 'claim'],
            ['', '', ''],
            ['  ', '  ', '  ']
        ]
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert result is None
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_pop_first_row_filter_mismatch_continues(self, mock_verify):
        """Deve pular linhas que não atendem filtros."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'status', 'claim'],
            ['1', 'DONE', ''],
            ['2', 'PENDING', '']
        ]
        
        mapping = {'id': 0, 'status': 1, 'claim': 2}
        mock_verify.return_value = True
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {'status': 'PENDING'},
            'claim',
            'worker1'
        )
        
        assert result is not None
        assert result[0] == 3
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_pop_first_row_already_claimed_by_others(self, mock_verify):
        """Deve pular linhas já reivindicadas por outros workers."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name', 'claim'],
            ['1', 'Alice', 'worker2'],
            ['2', 'Bob', '']
        ]
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        mock_verify.return_value = True
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert result is not None
        assert result[0] == 3
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_pop_first_row_claim_update_error(self, mock_verify):
        """Deve continuar tentando se update_cell falhar."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name', 'claim'],
            ['1', 'Alice', '']
        ]
        mock_worksheet.update_cell.side_effect = [Exception('Error'), None]
        mock_verify.return_value = True
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert result is not None
        assert mock_worksheet.update_cell.call_count >= 2
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_pop_first_row_claim_stolen(self, mock_verify):
        """Deve tentar mesma linha novamente se ownership falhar na primeira tentativa."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name', 'claim'],
            ['1', 'Alice', ''],
            ['2', 'Bob', '']
        ]
        mock_verify.side_effect = [False, True]
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert mock_verify.call_count == 2
        assert result is not None
        assert result[0] == 2
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_pop_first_row_appends_claim_if_needed(self, mock_verify):
        """Deve adicionar claim à row_data se necessário."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name', 'claim'],
            ['1', 'Alice']  # Sem claim
        ]
        mock_verify.return_value = True
        
        mapping = {'id': 0, 'name': 1, 'claim': 2}
        
        result = pop_first_row_by_columns(
            mock_worksheet,
            mapping,
            {},
            'claim',
            'worker1'
        )
        
        assert result is not None
        assert len(result[1]) == 3
        assert result[1][2] == 'worker1'


# ============================================================================
# TESTES SELECT - Incluindo linhas missing
# ============================================================================

class TestSelectOperations:
    """Testes para select_first_by_columns e select_all_by_columns."""
    
    def test_select_first_by_columns_success(self):
        """Deve retornar primeira linha que satisfaz filtros."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'status'],
            ['1', 'PENDING'],
            ['2', 'DONE'],
            ['3', 'PENDING']
        ]
        
        mapping = {'id': 0, 'status': 1}
        
        result = select_first_by_columns(
            mock_worksheet,
            mapping,
            {'status': 'PENDING'}
        )
        
        assert result is not None
        assert result[0] == 2
        assert result[1] == ['1', 'PENDING']
    
    def test_select_first_by_columns_not_found(self):
        """Deve retornar None se nenhuma linha satisfizer filtros."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'status'],
            ['1', 'DONE'],
            ['2', 'DONE']
        ]
        
        mapping = {'id': 0, 'status': 1}
        
        result = select_first_by_columns(
            mock_worksheet,
            mapping,
            {'status': 'PENDING'}
        )
        
        assert result is None
    
    def test_select_first_empty_row_skipped(self):
        """Deve pular linhas vazias."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'status'],
            [],
            ['1', 'PENDING']
        ]
        
        mapping = {'id': 0, 'status': 1}
        
        result = select_first_by_columns(
            mock_worksheet,
            mapping,
            {'status': 'PENDING'}
        )
        
        assert result is not None
        assert result[0] == 3
    
    def test_select_all_by_columns_success(self):
        """Deve retornar todas linhas que satisfazem filtros."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'status'],
            ['1', 'PENDING'],
            ['2', 'DONE'],
            ['3', 'PENDING']
        ]
        
        mapping = {'id': 0, 'status': 1}
        
        result = select_all_by_columns(
            mock_worksheet,
            mapping,
            {'status': 'PENDING'}
        )
        
        assert len(result) == 2
        assert result[0] == (2, ['1', 'PENDING'])
        assert result[1] == (4, ['3', 'PENDING'])
    
    def test_select_all_by_columns_empty(self):
        """Deve retornar lista vazia se nenhuma linha satisfizer."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'status'],
            ['1', 'DONE']
        ]
        
        mapping = {'id': 0, 'status': 1}
        
        result = select_all_by_columns(
            mock_worksheet,
            mapping,
            {'status': 'PENDING'}
        )
        
        assert result == []


# ============================================================================
# TESTES APPEND
# ============================================================================

class TestAppendOperations:
    """Testes para append_row e append_rows."""
    
    def test_append_row_success(self):
        """Deve adicionar linha com sucesso."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        append_row(mock_worksheet, ['1', 'Alice', 'PENDING'])
        
        mock_worksheet.append_row.assert_called_once_with(['1', 'Alice', 'PENDING'])
    
    def test_append_rows_success(self):
        """Deve adicionar múltiplas linhas com sucesso."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        rows = [
            ['1', 'Alice', 'PENDING'],
            ['2', 'Bob', 'DONE']
        ]
        
        append_rows(mock_worksheet, rows)
        
        mock_worksheet.append_rows.assert_called_once_with(rows)
    
    def test_append_rows_empty_list(self):
        """Não deve fazer nada se lista estiver vazia."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        append_rows(mock_worksheet, [])
        
        mock_worksheet.append_rows.assert_not_called()


# ============================================================================
# TESTES GET - Incluindo linhas missing
# ============================================================================

class TestGetOperations:
    """Testes para get_row, get_rows e get_next_valid_row."""
    
    def test_get_row_success(self):
        """Deve retornar valores da linha."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.row_values.return_value = ['1', 'Alice', 'PENDING']
        
        result = get_row(mock_worksheet, 2)
        
        assert result == ['1', 'Alice', 'PENDING']
    
    def test_get_row_empty(self):
        """Deve retornar None se linha estiver vazia."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.row_values.return_value = []
        
        result = get_row(mock_worksheet, 2)
        
        assert result is None
    
    def test_get_rows_success(self):
        """Deve retornar múltiplas linhas."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get.return_value = [
            ['1', 'Alice'],
            ['2', 'Bob']
        ]
        
        result = get_rows(mock_worksheet, 2, 3)
        
        assert len(result) == 2
        assert result[0] == ['1', 'Alice']
        assert result[1] == ['2', 'Bob']
    
    def test_get_rows_invalid_range(self):
        """Deve retornar lista vazia se range for inválido."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        result = get_rows(mock_worksheet, 5, 3)
        
        assert result == []
    
    def test_get_next_valid_row_success(self):
        """Deve retornar primeira linha não vazia."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name'],
            ['', ''],
            ['1', 'Alice']
        ]
        
        result = get_next_valid_row(mock_worksheet)
        
        assert result is not None
        assert result[0] == 3
        assert result[1] == ['1', 'Alice']
    
    def test_get_next_valid_row_none_found(self):
        """Deve retornar None se não houver linhas válidas."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.get_all_values.return_value = [
            ['id', 'name'],
            ['', ''],
            ['  ', '  ']
        ]
        
        result = get_next_valid_row(mock_worksheet)
        
        assert result is None


# ============================================================================
# TESTES UPDATE E DELETE
# ============================================================================

class TestUpdateAndDeleteOperations:
    """Testes para update_row e delete_row."""
    
    def test_update_row_success(self):
        """Deve atualizar linha com sucesso."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        result = update_row(mock_worksheet, 2, ['1', 'Alice', 'DONE'])
        
        assert result is True
        mock_worksheet.update.assert_called_once_with('2:2', [['1', 'Alice', 'DONE']])
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_update_row_with_ownership(self, mock_verify):
        """Deve verificar ownership antes de atualizar."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_verify.return_value = True
        
        result = update_row(
            mock_worksheet,
            2,
            ['1', 'Alice', 'worker1'],
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result is True
        mock_verify.assert_called_once()
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_update_row_ownership_failed(self, mock_verify):
        """Deve retornar False se ownership falhar."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_verify.return_value = False
        
        result = update_row(
            mock_worksheet,
            2,
            ['1', 'Alice', 'worker2'],
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result is False
    
    def test_delete_row_success(self):
        """Deve fazer soft delete com sucesso."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_count = 3
        
        result = delete_row(mock_worksheet, 2)
        
        assert result is True
        mock_worksheet.update.assert_called_once_with('2:2', [['', '', '']])
    
    def test_delete_row_no_columns(self):
        """Deve fazer soft delete mesmo com col_count=0 (retorna lista vazia)."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_count = 0
        
        result = delete_row(mock_worksheet, 2)
        
        # Com 0 colunas, cria empty_row [] e update funciona
        assert result is True
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_delete_row_ownership_failed(self, mock_verify):
        """Deve retornar False se ownership falhar."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_count = 3
        mock_verify.return_value = False
        
        result = delete_row(
            mock_worksheet,
            2,
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result is False
    
    def test_delete_row_exception(self):
        """Deve retornar False se houver exceção."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.col_count = 3
        mock_worksheet.update.side_effect = Exception('API Error')
        
        result = delete_row(mock_worksheet, 2)
        
        assert result is False


# ============================================================================
# TESTES MOVE ROW - Incluindo linhas missing
# ============================================================================

class TestMoveRow:
    """Testes para move_row."""
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_move_row_success(self, mock_verify):
        """Deve mover linha com sucesso."""
        mock_from_ws = Mock()
        mock_from_ws.title = 'FromSheet'
        mock_from_ws.col_count = 3
        mock_from_ws.row_values.return_value = ['1', 'Alice', 'worker1']
        
        mock_to_ws = Mock()
        mock_to_ws.title = 'ToSheet'
        
        mock_verify.return_value = True
        
        result = move_row(
            source_ws=mock_from_ws,
            target_ws=mock_to_ws,
            row_number=2,
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result == ['1', 'Alice', 'worker1']
        mock_to_ws.append_row.assert_called_once()
        mock_from_ws.update.assert_called()
    
    @patch('orc.gateway.operations._verify_ownership')
    def test_move_row_ownership_failed(self, mock_verify):
        """Deve retornar None se ownership falhar."""
        mock_from_ws = Mock()
        mock_from_ws.title = 'FromSheet'
        mock_to_ws = Mock()
        mock_to_ws.title = 'ToSheet'
        
        mock_verify.return_value = False
        
        result = move_row(
            source_ws=mock_from_ws,
            target_ws=mock_to_ws,
            row_number=2,
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result is None
    
    @patch('orc.gateway.operations._verify_ownership')
    @patch('orc.gateway.operations.append_row')
    @patch('orc.gateway.operations.delete_row')
    def test_move_row_empty_row(self, mock_delete, mock_append, mock_verify):
        """Deve retornar None se linha estiver vazia."""
        mock_from_ws = Mock()
        mock_from_ws.title = 'FromSheet'
        mock_from_ws.col_count = 3
        mock_from_ws.row_values.return_value = []
        
        mock_to_ws = Mock()
        mock_to_ws.title = 'ToSheet'
        
        mock_verify.return_value = True
        
        result = move_row(
            source_ws=mock_from_ws,
            target_ws=mock_to_ws,
            row_number=2,
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result is None
        mock_append.assert_not_called()
    
    @patch('orc.gateway.operations._verify_ownership')
    @patch('orc.gateway.operations.append_row')
    def test_move_row_claim_mismatch(self, mock_append, mock_verify):
        """Deve retornar None se claim não bater."""
        mock_from_ws = Mock()
        mock_from_ws.title = 'FromSheet'
        mock_from_ws.col_count = 3
        mock_from_ws.row_values.return_value = ['1', 'Alice', 'worker2']
        
        mock_to_ws = Mock()
        mock_to_ws.title = 'ToSheet'
        
        mock_verify.return_value = True
        
        result = move_row(
            source_ws=mock_from_ws,
            target_ws=mock_to_ws,
            row_number=2,
            claim_column_index=2,
            claim_value='worker1'
        )
        
        assert result is None
        mock_append.assert_not_called()
    



# ============================================================================
# TESTES GET_ROWS_BY_NUMBERS
# ============================================================================

class TestGetRowsByNumbers:
    """Testes para get_rows_by_numbers."""
    
    def test_get_rows_by_numbers_success(self):
        """Deve retornar linhas especificadas usando row_values."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        mock_worksheet.row_values.side_effect = lambda n: ['1', 'Alice'] if n == 2 else ['2', 'Bob']
        
        result = get_rows_by_numbers(mock_worksheet, [2, 3])
        
        assert len(result) == 2
        assert result[0] == ['1', 'Alice']
        assert result[1] == ['2', 'Bob']
    
    def test_get_rows_by_numbers_empty_list(self):
        """Deve retornar lista vazia se não houver row_numbers."""
        mock_worksheet = Mock()
        mock_worksheet.title = 'TestSheet'
        
        result = get_rows_by_numbers(mock_worksheet, [])
        
        assert result == []
