"""Testes unitários para o módulo leader."""
from unittest.mock import Mock, patch

from orc.gateway.leader import _ensure_election_worksheet, ELECTION_SHEET_NAME, ELECTION_HEADER


class TestEnsureElectionWorksheet:
    """Testes para _ensure_election_worksheet."""
    
    @patch('orc.gateway.leader.get_worksheet')
    def test_ensure_election_worksheet_exists(self, mock_get_worksheet):
        """Deve retornar worksheet existente."""
        mock_worksheet = Mock()
        mock_spreadsheet = Mock()
        mock_get_worksheet.return_value = mock_worksheet
        
        result = _ensure_election_worksheet(mock_spreadsheet)
        
        assert result == mock_worksheet
        mock_get_worksheet.assert_called_once_with(
            spreadsheet=mock_spreadsheet,
            worksheet_name=ELECTION_SHEET_NAME,
            header=ELECTION_HEADER,
            replace_header=False,
            create=True
        )
    
    def test_ensure_election_worksheet_no_errors(self):
        """Deve retornar worksheet sem erros se get_worksheet funcionar."""        
        # Este teste apenas verifica que a função chama get_worksheet corretamente
        # O retry é testado em test_worksheet.py
        mock_spreadsheet = Mock()
        
        with patch('orc.gateway.leader.get_worksheet') as mock_get_worksheet:
            mock_worksheet = Mock()
            mock_get_worksheet.return_value = mock_worksheet
            
            result = _ensure_election_worksheet(mock_spreadsheet)
            
            assert result == mock_worksheet


class TestTryAcquireLeadership:
    """Testes para try_acquire_leadership."""
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.append_row')
    def test_acquire_new_election(self, mock_append, mock_select, mock_mapping, mock_ensure):
        """Deve criar nova eleição se não existir."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        mock_select.return_value = None  # Eleição não existe
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is True
        mock_append.assert_called_once()
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    @patch('orc.gateway.leader.get_row')
    @patch('orc.gateway.leader.time')
    def test_acquire_expired_leadership(self, mock_time, mock_get_row, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve adquirir liderança se anterior expirou."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        mock_time.sleep = Mock()
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Líder expirado (expiration = 500, now = 1000)
        mock_select.return_value = (2, ['test_election', 'worker2', '400.0', '500.0', 'ACTIVE'])
        mock_get_row.return_value = ['test_election', 'worker1', '1000.0', '1060.0', 'ACTIVE']
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is True
        mock_update.assert_called_once()
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.time')
    def test_acquire_leadership_already_active(self, mock_time, mock_select, mock_mapping, mock_ensure):
        """Não deve adquirir se já houver líder ativo."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Líder ativo (expiration = 2000, now = 1000)
        mock_select.return_value = (2, ['test_election', 'worker2', '900.0', '2000.0', 'ACTIVE'])
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is False
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    @patch('orc.gateway.leader.get_row')
    @patch('orc.gateway.leader.time')
    def test_acquire_leadership_exception(self, mock_time, mock_get_row, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve retornar False se houver exceção ao renovar."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        mock_time.sleep = Mock()
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Líder expirado mas get_row falha
        mock_select.return_value = (2, ['test_election', 'worker2', '400.0', '500.0', 'ACTIVE'])
        mock_get_row.side_effect = Exception('Error')
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is False
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    @patch('orc.gateway.leader.get_row')
    @patch('orc.gateway.leader.time')
    def test_acquire_leadership_renew_same_worker(self, mock_time, mock_get_row, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve renovar liderança se mesmo worker tentar novamente."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        mock_time.sleep = Mock()
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Mesmo worker já é líder
        mock_select.return_value = (2, ['test_election', 'worker1', '900.0', '2000.0', 'ACTIVE'])
        mock_get_row.return_value = ['test_election', 'worker1', '1000.0', '1060.0', 'ACTIVE']
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is True
        # Deve ter atualizado
        mock_update.assert_called_once()
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    @patch('orc.gateway.leader.get_row')
    @patch('orc.gateway.leader.time')
    def test_acquire_leadership_invalid_expiration(self, mock_time, mock_get_row, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve adquirir se timestamp de expiração for inválido."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        mock_time.sleep = Mock()
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Timestamp inválido (ValueError ao converter)
        mock_select.return_value = (2, ['test_election', 'worker2', '900.0', 'invalid', 'ACTIVE'])
        mock_get_row.return_value = ['test_election', 'worker1', '1000.0', '1060.0', 'ACTIVE']
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is True
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.time')
    def test_acquire_leadership_inactive_status(self, mock_time, mock_select, mock_mapping, mock_ensure):
        """Deve adquirir se status for inativo mesmo que não expirado."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        mock_time.sleep = Mock()
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Status INACTIVE (não expirado mas inativo)
        mock_select.return_value = (2, ['test_election', 'worker2', '900.0', '2000.0', 'INACTIVE'])
        
        with patch('orc.gateway.leader.update_row'):
            with patch('orc.gateway.leader.get_row') as mock_get_row:
                mock_get_row.return_value = ['test_election', 'worker1', '1000.0', '1060.0', 'ACTIVE']
                
                result = try_acquire_leadership(
                    Mock(),
                    'test_election',
                    'worker1',
                    ttl_seconds=60
                )
                
                assert result is True
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    @patch('orc.gateway.leader.get_row')
    @patch('orc.gateway.leader.time')
    def test_acquire_leadership_check_row_returns_different_leader(self, mock_time, mock_get_row, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve retornar False se verificação final mostrar líder diferente."""
        from orc.gateway.leader import try_acquire_leadership
        
        mock_time.time.return_value = 1000.0
        mock_time.sleep = Mock()
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Líder expirado
        mock_select.return_value = (2, ['test_election', 'worker2', '400.0', '500.0', 'ACTIVE'])
        # Mas verificação final mostra outro líder
        mock_get_row.return_value = ['test_election', 'worker3', '1000.0', '1060.0', 'ACTIVE']
        
        result = try_acquire_leadership(
            Mock(),
            'test_election',
            'worker1',
            ttl_seconds=60
        )
        
        assert result is False


class TestReleaseLeadership:
    """Testes para release_leadership."""
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    def test_release_leadership_success(self, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve liberar liderança com sucesso."""
        from orc.gateway.leader import release_leadership
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        mock_select.return_value = (2, ['test_election', 'worker1', '1000.0', '2000.0', 'ACTIVE'])
        
        release_leadership(
            Mock(),
            'test_election',
            'worker1'
        )
        
        mock_update.assert_called_once()
        # Verificar que status foi mudado para RELEASED
        call_args = mock_update.call_args[0]
        assert call_args[2][4] == 'RELEASED'
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    def test_release_leadership_not_leader(self, mock_select, mock_mapping, mock_ensure):
        """Não deve fazer nada se worker não for o líder."""
        from orc.gateway.leader import release_leadership
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Timestamp de Aquisição': 2,
            'Timestamp de Expiração': 3,
            'Status': 4
        }
        
        # Líder é outro worker
        mock_select.return_value = (2, ['test_election', 'worker2', '1000.0', '2000.0', 'ACTIVE'])
        
        # Não deve lançar exceção
        release_leadership(
            Mock(),
            'test_election',
            'worker1'
        )
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    def test_release_leadership_election_not_found(self, mock_select, mock_mapping, mock_ensure):
        """Não deve fazer nada se eleição não existir."""
        from orc.gateway.leader import release_leadership
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {'Nome da Eleição': 0}
        mock_select.return_value = None
        
        # Não deve lançar exceção
        release_leadership(
            Mock(),
            'nonexistent',
            'worker1'
        )
    
    @patch('orc.gateway.leader._ensure_election_worksheet')
    @patch('orc.gateway.leader.get_header_mapping')
    @patch('orc.gateway.leader.select_first_by_columns')
    @patch('orc.gateway.leader.update_row')
    def test_release_leadership_exception_silenced(self, mock_update, mock_select, mock_mapping, mock_ensure):
        """Deve silenciar exceção ao liberar liderança."""
        from orc.gateway.leader import release_leadership
        
        mock_ws = Mock()
        mock_ensure.return_value = mock_ws
        mock_mapping.return_value = {
            'Nome da Eleição': 0,
            'ID do Líder': 1,
            'Status': 4
        }
        mock_select.return_value = (2, ['test_election', 'worker1', '1000.0', '1060.0', 'ACTIVE'])
        mock_update.side_effect = Exception('Update failed')
        
        # Não deve levantar exceção
        release_leadership(Mock(), 'test_election', 'worker1')
