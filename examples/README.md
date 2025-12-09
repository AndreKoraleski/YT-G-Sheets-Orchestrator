# Exemplos de Uso

Esta pasta contém exemplos de como usar o YT G-Sheets Orchestrator.

## Exemplos Disponíveis

### 1. `basic_usage.py`

Exemplo básico demonstrando:
- Configuração do worker
- Loop de processamento simples
- Callback de processamento de vídeo
- Graceful shutdown

**Como executar:**
```bash
# Configure o .env primeiro
cp ../.env.example ../.env
# Edite o .env com suas credenciais

# Execute o exemplo
uv run python basic_usage.py
```

### 2. `advanced_usage.py`

Exemplo avançado demonstrando:
- Classe customizada de processamento
- Logging estruturado
- Tratamento de erros robusto
- Estatísticas de processamento
- Integração com serviços externos

**Como executar:**
```bash
uv run python advanced_usage.py
```

## Estrutura de um Worker Customizado

```python
from orc import Config, Orchestrator

def process_video(url: str) -> None:
    """Sua lógica de processamento aqui."""
    # 1. Extrair informações do vídeo
    # 2. Processar conforme necessário
    # 3. Salvar resultados
    pass

def main():
    config = Config()
    orchestrator = Orchestrator(config)
    
    while True:
        orchestrator.process_next_task(process_video)
        orchestrator.send_heartbeat()

if __name__ == "__main__":
    main()
```

## Dicas

1. **Heartbeat**: Sempre chame `send_heartbeat()` regularmente
2. **Error Handling**: Implemente try/except em sua callback
3. **Logging**: Use logging para rastrear processamento
4. **Graceful Shutdown**: Capture KeyboardInterrupt
5. **Idempotência**: Certifique-se que sua lógica pode ser reprocessada
