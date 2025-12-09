"""Configuração de testes pytest."""
import sys
from pathlib import Path

# Adicionar src ao path para importação dos módulos
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))
