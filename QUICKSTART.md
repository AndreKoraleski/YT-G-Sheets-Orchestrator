# Guia de Início Rápido

Este guia vai te ajudar a começar a usar o YT G-Sheets Orchestrator em até 15 minutos.

## 1. Instalação

```bash
uv pip install yt-gsheet-orchestrator
```

## 2. Setup do Google Cloud

### 2.1. Criar Service Account

1. Acesse o [Google Cloud Console](https://console.cloud.google.com)
2. Crie um novo projeto ou selecione um existente
3. Vá para **IAM & Admin** > **Service Accounts**
4. Clique em **Create Service Account**
5. Dê um nome (ex: `yt-orchestrator`)
6. Clique em **Create and Continue**
7. Pule as permissões opcionais
8. Clique em **Done**

### 2.2. Gerar Credenciais

1. Clique na service account criada
2. Vá para a aba **Keys**
3. Clique em **Add Key** > **Create new key**
4. Escolha **JSON** e clique em **Create**
5. Salve o arquivo JSON baixado em um local seguro

### 2.3. Habilitar APIs

1. Vá para **APIs & Services** > **Library**
2. Procure e habilite a **Google Sheets API**

## 3. Setup do Google Sheets

### 3.1. Criar Planilha

1. Crie uma nova [Google Sheets](https://sheets.google.com)
2. Compartilhe a planilha com o email da service account
   - Email está no arquivo JSON: `client_email`
   - Conceda permissão de **Editor**

### 3.2. Criar Abas

Crie as seguintes abas (sheet tabs) na planilha:

- `Workers`
- `Tasks`
- `Tasks History`
- `Tasks DLQ`
- `Sources`
- `Sources History`
- `Sources DLQ`
- `Leader Election`

**Dica**: Deixe todas as abas vazias - os headers serão criados automaticamente!

## 4. Configuração do Ambiente

Crie um arquivo `.env`:

```bash
# Nome único para este worker
WORKER_NAME=worker-01

# ID da planilha (copie da URL do Google Sheets)
# URL: https://docs.google.com/spreadsheets/d/ESTE_É_O_ID/edit
SPREADSHEET_ID=seu-spreadsheet-id-aqui

# Caminho para o arquivo JSON da service account
SERVICE_ACCOUNT_FILE=./path/to/service-account.json
```

## 5. Primeiro Worker

Crie um arquivo `my_worker.py`:

```python
from orc import Config, Orchestrator

def process_video(url: str) -> None:
    """Processa um vídeo do YouTube."""
    print(f"Processando: {url}")
    # Sua lógica aqui!

def main():
    config = Config()
    orchestrator = Orchestrator(config)
    
    print(f"Worker '{config.worker_name}' iniciado!")
    print("Pressione Ctrl+C para parar\n")
    
    while True:
        orchestrator.process_next_task(process_video)
        orchestrator.send_heartbeat()

if __name__ == "__main__":
    main()
```

Execute:

```bash
uv run python my_worker.py
```

## 6. Adicionar Sources

Na aba `Sources` da planilha, adicione URLs do YouTube:

| ID | URL | Nome | Quantidade de Vídeos | Timestamp de Reivindicação | Timestamp de Conclusão | Status | Worker Atribuído |
|----|-----|------|----------------------|---------------------------|------------------------|--------|------------------|
| | https://www.youtube.com/playlist?list=PLrAXtmErZgOeiKm4sgNOknGvNjby9efdf | | | | | PENDING | |
| | https://www.youtube.com/@PythonBrasil | | | | | PENDING | |

**Importante**: Deixe as outras colunas vazias - o sistema preenche automaticamente!

## 7. Verificar Funcionamento

1. **Aba Workers**: Verifique que seu worker está com `Status = ACTIVE`
2. **Aba Tasks**: Após processar sources, verá tasks individuais
3. **Aba Tasks History**: Tasks completadas aparecem aqui
4. **Aba Sources History**: Sources completadas aparecem aqui

## 8. Escalar Horizontalmente

Para adicionar mais workers:

```bash
# Terminal 1
WORKER_NAME=worker-01 uv run python my_worker.py

# Terminal 2
WORKER_NAME=worker-02 uv run python my_worker.py

# Terminal 3
WORKER_NAME=worker-03 uv run python my_worker.py
```

O sistema automaticamente:
- Distribui trabalho entre workers
- Ajusta rate limiting
- Elege líder para processar sources

## Próximos Passos

- 📖 Leia o [README.md](README.md) completo
- 🚀 Veja [exemplos avançados](examples/)
- 📦 Confira [guia de deployment](DEPLOYMENT.md)
- 🔒 Revise [práticas de segurança](SECURITY.md)

## Troubleshooting

### Worker não inicia

✅ **Solução**: Verifique variáveis de ambiente no `.env`

### "Permission denied" ao acessar planilha

✅ **Solução**: Compartilhe a planilha com o email da service account

### Tasks não são processadas

✅ **Solução**: 
- Verifique se há sources pendentes na aba `Sources`
- Verifique se o worker está `ACTIVE` na aba `Workers`

### Erros de quota da API

✅ **Solução**: 
- Reduza número de workers
- Aumente delays entre operações
- Solicite aumento de quota no Google Cloud

## Suporte

Encontrou problemas? [Abra uma issue](https://github.com/AndreKoraleski/yt-gsheet-orchestrator/issues)!
