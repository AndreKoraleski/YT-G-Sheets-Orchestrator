"""
Exemplo: Demonstração de Error Handling e DLQ.

Este exemplo mostra como exceções são automaticamente capturadas
e registradas na Dead Letter Queue (DLQ) pelo orchestrator.
"""

import time

from dotenv import load_dotenv

from orc import Config, Orchestrator

# Carrega variáveis de ambiente do .env
load_dotenv()


def process_video_with_errors(url: str) -> None:
    """
    Callback que demonstra diferentes tipos de erros.

    IMPORTANTE: Este callback propositalmente levanta exceções para
    demonstrar o sistema de DLQ. Em produção, deixe que erros reais
    sejam propagados naturalmente.

    Args:
        url: URL do vídeo do YouTube a ser processado

    Raises:
        ValueError: Se a URL for inválida
        RuntimeError: Se houver erro no processamento
    """
    print(f"Processando vídeo: {url}")

    # Simula validação que pode falhar
    if not url.startswith("https://"):
        # Esta exceção será capturada pelo orchestrator
        # e a task será movida para DLQ com a mensagem:
        # "URL deve começar com https://"
        raise ValueError("URL deve começar com https://")

    # Simula erro de processamento baseado no conteúdo da URL
    if "error" in url.lower():
        # Demonstra erro de runtime
        raise RuntimeError(f"Erro simulado no processamento de {url}")

    # Simula processamento normal
    time.sleep(1)
    print(f"✅ Vídeo processado com sucesso: {url}")


def main():
    """
    Função principal que demonstra error handling.

    O orchestrator automaticamente:
    1. Captura qualquer exceção levantada pelo callback
    2. Registra o erro no log
    3. Move a task para a DLQ com str(e)
    4. Continua processando outras tasks
    """
    config = Config()
    orchestrator = Orchestrator(config)

    print("=" * 70)
    print("Exemplo: Error Handling e DLQ")
    print("=" * 70)
    print(f"Planilha: {config.spreadsheet_id}")
    print(f"Worker: {config.worker_name}")
    print()
    print("Este exemplo demonstra como erros são capturados:")
    print("   - URLs inválidas → ValueError → DLQ")
    print("   - Erros de processamento → RuntimeError → DLQ")
    print("   - Tasks bem-sucedidas → History")
    print()
    print("Verifique as abas DLQ no Google Sheets para ver os erros!")
    print("=" * 70)
    print()

    task_count = 0

    try:
        while task_count < 10:  # Processa até 10 tasks como exemplo
            if orchestrator.process_next_task(process_video_with_errors):
                task_count += 1
                print(f"Processadas: {task_count} tasks\n")
            else:
                print("Nenhuma task disponível, aguardando...")
                time.sleep(5)

            orchestrator.send_heartbeat()

    except KeyboardInterrupt:
        print("\n\nRecebido sinal de parada...")
    finally:
        print("\nVerifique a DLQ no Google Sheets!")
        print(f"Total processado: {task_count} tasks")


if __name__ == "__main__":
    main()
