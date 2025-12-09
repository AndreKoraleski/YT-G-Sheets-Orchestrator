# Contributing to YT G-Sheets Orchestrator

Obrigado por considerar contribuir para este projeto!

## Como Contribuir

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

## Desenvolvimento

### Setup do Ambiente

```bash
# Clone o repositório
git clone https://github.com/AndreKoraleski/yt-gsheet-orchestrator.git
cd yt-gsheet-orchestrator

# Instale dependências de desenvolvimento
uv pip install -e ".[dev]"
```

### Rodando Testes

```bash
# Todos os testes
uv run pytest

# Com coverage
uv run pytest --cov=orc --cov-report=html

# Testes específicos
uv run pytest tests/test_orchestrator.py
```

### Code Style

- Use type hints em todas as funções
- Siga PEP 8 para formatação
- Docstrings para módulos e funções públicas
- Mantenha cobertura de testes acima de 60%

## Reportando Bugs

Abra uma issue no GitHub incluindo:
- Descrição clara do problema
- Passos para reproduzir
- Comportamento esperado vs. atual
- Versão do Python e dependências

## Sugerindo Features

Abra uma issue no GitHub com:
- Descrição detalhada da feature
- Casos de uso
- Possível implementação (opcional)
