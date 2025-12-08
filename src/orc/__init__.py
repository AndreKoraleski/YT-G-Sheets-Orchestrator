"""
Módulo de inicialização para o pacote orc.

Este módulo importa e expõe:

- Config: Classe de configuração para o trabalhador ORC. Uso opcional, pois as configurações podem ser carregadas diretamente das variáveis de ambiente.
"""

from .config import Config

__all__ = ['Config']