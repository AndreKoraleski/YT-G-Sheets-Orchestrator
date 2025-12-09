"""Testes unitários para o módulo config."""
import pytest
import os
from unittest.mock import patch

from orc.config import Config


class TestConfig:
    """Testes para a classe Config."""
    
    def test_config_with_env_variables(self):
        """Deve carregar configurações das variáveis de ambiente."""
        with patch.dict(os.environ, {
            'WORKER_NAME': 'test_worker',
            'SPREADSHEET_ID': 'test_sheet_id',
            'SERVICE_ACCOUNT_FILE': 'test_account.json'
        }):
            config = Config()
            
            assert config.worker_name == 'test_worker'
            assert config.spreadsheet_id == 'test_sheet_id'
            assert config.service_account_file == 'test_account.json'
    
    def test_config_with_direct_values(self):
        """Deve aceitar valores diretos ao invés de variáveis de ambiente."""
        config = Config(
            worker_name='direct_worker',
            spreadsheet_id='direct_sheet',
            service_account_file='direct_account.json'
        )
        
        assert config.worker_name == 'direct_worker'
        assert config.spreadsheet_id == 'direct_sheet'
        assert config.service_account_file == 'direct_account.json'
    
    def test_config_missing_worker_name_raises_error(self):
        """Deve lançar erro se WORKER_NAME não estiver definido."""
        with patch.dict(os.environ, {
            'SPREADSHEET_ID': 'test_sheet_id',
            'SERVICE_ACCOUNT_FILE': 'test_account.json'
        }, clear=True):
            with pytest.raises(ValueError, match="WORKER_NAME"):
                Config()
    
    def test_config_missing_spreadsheet_id_raises_error(self):
        """Deve lançar erro se SPREADSHEET_ID não estiver definido."""
        with patch.dict(os.environ, {
            'WORKER_NAME': 'test_worker',
            'SERVICE_ACCOUNT_FILE': 'test_account.json'
        }, clear=True):
            with pytest.raises(ValueError, match="SPREADSHEET_ID"):
                Config()
    
    def test_config_missing_service_account_file_raises_error(self):
        """Deve lançar erro se SERVICE_ACCOUNT_FILE não estiver definido."""
        with patch.dict(os.environ, {
            'WORKER_NAME': 'test_worker',
            'SPREADSHEET_ID': 'test_sheet_id'
        }, clear=True):
            with pytest.raises(ValueError, match="SERVICE_ACCOUNT_FILE"):
                Config()
    
    def test_config_is_frozen(self):
        """Deve ser imutável (frozen dataclass)."""
        config = Config(
            worker_name='test_worker',
            spreadsheet_id='test_sheet',
            service_account_file='test_account.json'
        )
        
        with pytest.raises(Exception):  # FrozenInstanceError
            config.worker_name = 'new_name'
    
    def test_config_direct_overrides_env(self):
        """Valores diretos devem ter prioridade sobre variáveis de ambiente."""
        with patch.dict(os.environ, {
            'WORKER_NAME': 'env_worker',
            'SPREADSHEET_ID': 'env_sheet',
            'SERVICE_ACCOUNT_FILE': 'env_account.json'
        }):
            config = Config(
                worker_name='direct_worker',
                spreadsheet_id='direct_sheet',
                service_account_file='direct_account.json'
            )
            
            assert config.worker_name == 'direct_worker'
            assert config.spreadsheet_id == 'direct_sheet'
            assert config.service_account_file == 'direct_account.json'
