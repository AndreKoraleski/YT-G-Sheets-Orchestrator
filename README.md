# YT G-Sheets Orchestrator

Sistema de orquestração distribuída para processar playlists e canais do YouTube usando Google Sheets como camada de coordenação.

[![PyPI version](https://img.shields.io/pypi/v/yt-gsheet-orchestrator.svg)](https://pypi.org/project/yt-gsheet-orchestrator/)
[![Python Support](https://img.shields.io/pypi/pyversions/yt-gsheet-orchestrator.svg)](https://pypi.org/project/yt-gsheet-orchestrator/)
[![License](https://img.shields.io/github/license/AndreKoraleski/yt-gsheet-orchestrator.svg)](https://github.com/AndreKoraleski/yt-gsheet-orchestrator/blob/main/LICENSE)
[![CI](https://github.com/AndreKoraleski/yt-gsheet-orchestrator/workflows/CI/badge.svg)](https://github.com/AndreKoraleski/yt-gsheet-orchestrator/actions)

---

**📚 Documentação**: [Início Rápido](QUICKSTART.md) | [Exemplos](examples/) | [Deployment](DEPLOYMENT.md) | [Contribuindo](CONTRIBUTING.md) | [Changelog](CHANGELOG.md)

---

## Visão Geral

Este projeto implementa um sistema de processamento distribuído de tarefas onde múltiplos workers se coordenam através do Google Sheets para extrair metadados de fontes do YouTube e processar vídeos individuais. Cada worker opera independentemente com sua própria service account, usando eleição de liderança e ownership baseado em claim para distribuir trabalho com segurança entre o cluster.

## Arquitetura

O sistema usa um padrão de pipeline com três tabelas tanto para sources quanto para tasks:

- **Fila Principal**: Itens pendentes aguardando reivindicação
- **History**: Itens completados com sucesso
- **DLQ (Dead Letter Queue)**: Itens falhados com mensagens de erro

Workers se registram em uma tabela Workers com IDs únicos, heartbeats e estatísticas de processamento. Eleição de liderança é usada para coordenar o processamento de sources, enquanto tasks individuais podem ser reivindicadas por qualquer worker ativo.

## Requisitos

- Python 3.10+
- Google Cloud service account com acesso à API do Sheets
- yt-dlp para extração de metadados do YouTube

## Instalação

### Via PyPI

```bash
# Usando uv (recomendado)
uv pip install yt-gsheet-orchestrator

# Ou usando pip
pip install yt-gsheet-orchestrator
```

### Para Desenvolvimento

```bash
# Clone o repositório
git clone https://github.com/AndreKoraleski/yt-gsheet-orchestrator.git
cd yt-gsheet-orchestrator

# Instale com dependências de desenvolvimento
uv pip install -e ".[dev]"
```

## Configuração

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
WORKER_NAME=worker-1
SPREADSHEET_ID=your-spreadsheet-id
SERVICE_ACCOUNT_FILE=path/to/service-account.json
```

### Variáveis de Ambiente

- `WORKER_NAME`: Identificador único para esta instância de worker (obrigatório)
- `SPREADSHEET_ID`: ID da planilha do Google Sheets (obrigatório)
- `SERVICE_ACCOUNT_FILE`: Caminho para o arquivo JSON da service account do Google Cloud (obrigatório)

## Uso

### Executando um Worker

```bash
# Com uv
uv run python -m orc

# Ou diretamente se instalado no ambiente
python -m orc
```

O worker irá:
1. Registrar ou recuperar sua sessão na tabela Workers
2. Tentar reivindicar tasks disponíveis da fila Tasks
3. Se não houver tasks, adquirir liderança para processar sources
4. Extrair metadados de URLs do YouTube e criar novas tasks
5. Processar tasks chamando a função callback configurada
6. Realizar shutdown gracioso em SIGINT/SIGTERM

### Uso Programático

```python
from orc import Config, Orchestrator

# Inicializa com variáveis de ambiente
config = Config()
orchestrator = Orchestrator(config)

# Define callback de processamento de task
def process_video(url: str) -> None:
    print(f"Processando: {url}")
    # Sua lógica de processamento aqui

# Processa tasks em loop
while orchestrator.process_next_task(process_video):
    orchestrator.send_heartbeat()
```

### Adicionando Sources

Adicione URLs de playlists ou canais do YouTube diretamente na tabela Sources do Google Sheets:

| ID | URL | Nome | Quantidade de Vídeos | Timestamp de Reivindicação | Timestamp de Conclusão | Status | Worker Atribuído |
|----|-----|------|----------------------|---------------------------|------------------------|--------|------------------|
| | https://youtube.com/playlist?list=... | | | | | PENDING | |

O sistema automaticamente irá:
- Atribuir um UUID à source
- Extrair metadados usando yt-dlp
- Criar tasks individuais para cada vídeo
- Mover a source para History após conclusão

## Estrutura do Google Sheets

### Tabela Workers

Rastreia workers ativos e suas estatísticas de processamento.

**Cabeçalhos**: ID do Worker, Nome do Worker, Último Heartbeat, Status, Tarefas Processadas, Fontes Processadas

### Tabelas Tasks

- **Tasks**: Fila principal com tasks pendentes
- **Tasks History**: Tasks processadas com sucesso
- **Tasks DLQ**: Tasks falhadas com mensagens de erro

**Cabeçalhos**: ID, ID da Fonte, URL, Nome, Duração, Timestamp de Criação, Timestamp de Reivindicação, Timestamp de Conclusão, Status, Worker Atribuído

### Tabelas Sources

- **Sources**: Fila principal com sources pendentes
- **Sources History**: Sources processadas com sucesso
- **Sources DLQ**: Sources falhadas com mensagens de erro

**Cabeçalhos**: ID, URL, Nome, Quantidade de Vídeos, Timestamp de Reivindicação, Timestamp de Conclusão, Status, Worker Atribuído

### Tabela de Eleição de Líderes

Coordena liderança distribuída para processamento de sources.

**Cabeçalhos**: Nome da Eleição, Worker ID, Expira Em

## Funcionalidades

### Coordenação Distribuída

- **Registro de Workers**: Cada worker mantém uma sessão única com persistência de UUID
- **Eleição de Liderança**: Coordenação baseada em lease para processamento de sources (TTL de 5 minutos)
- **Ownership por Claim**: Workers reivindicam tasks/sources usando operações atômicas
- **Deduplicação**: Verificação automática contra History e DLQ antes do processamento

### Rate Limiting

O sistema implementa rate limiting dinâmico no nível do gateway:

- Rate limit base: 1.0 segundo entre chamadas de API
- Jitter escalável: Aumenta proporcionalmente com workers ativos
  - 1 worker: sem jitter
  - 2 workers: até 0.5s de jitter
  - 5 workers: até 2.0s de jitter

Isso previne esgotamento de quota da API enquanto permite que múltiplos workers operem eficientemente.

### Tolerância a Falhas

- **Shutdown Gracioso**: Handlers de SIGINT/SIGTERM marcam workers como INACTIVE e liberam liderança
- **Retry Automático**: Erros transientes são retentados com backoff exponencial
- **Dead Letter Queue**: Falhas persistentes são movidas para DLQ com detalhes do erro
- **Recuperação de Sessão**: Workers podem retomar sua sessão anterior ao reiniciar

### Extração de Metadados

Usa yt-dlp para extrair:
- IDs de vídeo diretamente das URLs do YouTube (formato de 11 caracteres)
- Nomes de playlists/canais e contagens de vídeos
- Títulos e durações de vídeos individuais

## Desenvolvimento

### Executando Testes

```bash
# Executar todos os testes
uv run pytest

# Executar com relatório de coverage
uv run pytest --cov=orc --cov-report=html
```

Cobertura de testes atual: 60% (94 testes)

### Publicando no PyPI

```bash
# Instalar dependências de build
uv pip install build twine

# Criar build
python -m build

# Upload para Test PyPI (opcional)
twine upload --repository testpypi dist/*

# Upload para PyPI
twine upload dist/*
```

### Estrutura do Projeto

```
src/orc/
├── __init__.py           # API pública (Config, Orchestrator)
├── __version__.py        # Versão do pacote
├── config.py             # Gerenciamento de configuração
├── orchestrator.py       # Lógica principal de orquestração
├── gateway/              # Operações do Google Sheets
│   ├── connection.py     # Conexão com planilha
│   ├── worksheet.py      # Gerenciamento de abas
│   ├── operations.py     # Operações CRUD
│   ├── leader.py         # Eleição de liderança
│   └── _retry.py         # Retry e rate limiting
└── tables/               # Lógica específica de tabelas
    ├── worker_table.py   # Gerenciamento de workers
    ├── task_table.py     # Operações da fila de tasks
    └── source_table.py   # Operações da fila de sources
```

## Licença

MIT License - Veja o arquivo LICENSE para detalhes.
