"""
Exemplo básico de uso do YT G-Sheets Orchestrator.

Este script demonstra como configurar e executar um worker
que processa vídeos do YouTube de forma distribuída.
"""

import time

from dotenv import load_dotenv

from orc import Config, Orchestrator

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()


def process_video(url: str) -> None:
    """
    Callback de processamento de vídeo.

    Esta função é chamada para cada task de vídeo que o worker processa.
    Implemente sua lógica customizada aqui.

    Args:
        url: URL do vídeo do YouTube a ser processado

    Raises:
        Exception: Se houver erro no processamento, a exceção será capturada
                   pelo orchestrator e a task será movida para a DLQ com a
                   mensagem de erro.
    """
    print(f"📹 Processando vídeo: {url}")

    # IMPORTANTE: Se algo der errado, LEVANTE uma exceção!
    # O orchestrator capturará e moverá para DLQ automaticamente
    if not url.startswith("https://"):
        raise ValueError(f"URL inválida: {url}")

    # Exemplo: Baixar metadados adicionais, transcrições, etc.
    # Exemplo: Fazer upload para S3, processar com IA, etc.

    # Simula processamento
    time.sleep(2)

    print(f"✅ Vídeo processado com sucesso: {url}")


def main():
    """Função principal."""
    # Inicializa configuração
    config = Config()

    # Cria instância do orchestrator
    orchestrator = Orchestrator(config)

    print("=" * 60)
    print(f"🚀 Worker '{config.worker_name}' iniciado")
    print(f"📊 Planilha: {config.spreadsheet_id}")
    print(f"🔐 Service Account: {config.service_account_file}")
    print("=" * 60)
    print("\n⏳ Aguardando tasks...\n")
    print("💡 Pressione Ctrl+C para parar graciosamente\n")

    # Loop principal de processamento
    task_count = 0
    while True:
        try:
            # Processa próxima task disponível
            if orchestrator.process_next_task(process_video):
                task_count += 1
                print(f"📊 Total processado: {task_count} tasks\n")
            else:
                # Nenhuma task disponível
                print("⏸️  Nenhuma task disponível, aguardando...")
                time.sleep(5)

            # Envia heartbeat para manter status ACTIVE
            orchestrator.send_heartbeat()

        except KeyboardInterrupt:
            print("\n\n🛑 Recebido sinal de parada...")
            print("🧹 Realizando cleanup gracioso...")
            break
        except Exception as e:
            print(f"❌ Erro no processamento: {e}")
            print("🔄 Continuando...")
            time.sleep(5)

    print("\n✨ Worker finalizado com sucesso")
    print(f"📈 Total de tasks processadas: {task_count}")


if __name__ == "__main__":
    main()
