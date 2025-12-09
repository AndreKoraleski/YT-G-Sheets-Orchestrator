# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [0.1.0] - 2025-12-09

### Adicionado
- Sistema de orquestração distribuída usando Google Sheets
- Suporte para processamento de playlists e canais do YouTube
- Eleição de liderança com TTL de 5 minutos
- Rate limiting dinâmico proporcional ao número de workers
- Graceful shutdown com handlers de SIGINT/SIGTERM
- Pipeline de 3 tabelas (Main, History, DLQ) para tasks e sources
- Extração de metadados com yt-dlp
- Rastreamento de workers ativos com heartbeat
- Deduplicação automática contra History e DLQ
- Retry automático com backoff exponencial
- 94 testes com 60% de cobertura
- Suporte para Python 3.10+

### Segurança
- Autenticação via Google Cloud Service Account
- Operações atômicas para ownership por claim
