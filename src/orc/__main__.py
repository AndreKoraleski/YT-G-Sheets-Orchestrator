"""Ponto de entrada para execução do módulo como script."""

import sys
from .config import Config
from .orchestrator import Orchestrator


def main():
    """Função principal para rodar o orchestrator."""
    try:
        config = Config()
        orchestrator = Orchestrator(config)

        def process_video(url: str) -> None:
            """Callback padrão para processamento de vídeos."""
            print(f"Processando vídeo: {url}")
            # Implementação customizada deve ser adicionada aqui

        print(f"Worker '{config.worker_name}' iniciado")
        print(f"Planilha: {config.spreadsheet_id}")
        print("Pressione Ctrl+C para parar graciosamente\n")

        while True:
            if not orchestrator.process_next_task(process_video):
                print("Nenhuma task disponível, aguardando...")
            orchestrator.send_heartbeat()

    except KeyboardInterrupt:
        print("\nParando worker...")
        sys.exit(0)
    except Exception as e:
        print(f"Erro fatal: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
