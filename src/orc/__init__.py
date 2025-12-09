"""
YT G-Sheets Orchestrator

Sistema de orquestração distribuída para processar playlists e canais do YouTube 
usando Google Sheets como camada de coordenação.

Este módulo expõe as principais classes para uso externo:

- Config: Classe de configuração para o trabalhador ORC
- Orchestrator: Orquestrador principal para processamento de tasks
"""

from .__version__ import __version__
from .config import Config
from .orchestrator import Orchestrator

__all__ = [
    '__version__',
    'Config',
    'Orchestrator',
]