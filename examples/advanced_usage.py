"""
Exemplo avançado: Worker com processamento customizado.

Demonstra integração com outros serviços e tratamento de erros.
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from orc import Config, Orchestrator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class VideoProcessor:
    """Processador de vídeos com lógica customizada."""
    
    def __init__(self):
        """Inicializa o processador."""
        self.processed_count = 0
        self.error_count = 0
        
    def process(self, url: str) -> bool:
        """
        Processa um vídeo do YouTube.
        
        Args:
            url: URL do vídeo
            
        Returns:
            True se processado com sucesso, False caso contrário
        """
        try:
            logger.info(f"Iniciando processamento: {url}")
            
            # 1. Extrair ID do vídeo
            video_id = self._extract_video_id(url)
            logger.debug(f"Video ID: {video_id}")
            
            # 2. Buscar metadados adicionais
            metadata = self._fetch_metadata(video_id)
            logger.debug(f"Metadata: {json.dumps(metadata, indent=2)}")
            
            # 3. Processar vídeo (exemplo: transcrição, análise, etc.)
            result = self._process_video(video_id, metadata)
            
            # 4. Salvar resultados
            self._save_results(video_id, result)
            
            self.processed_count += 1
            logger.info(f"✅ Processado com sucesso: {url}")
            return True
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ Erro ao processar {url}: {e}", exc_info=True)
            return False
    
    def _extract_video_id(self, url: str) -> str:
        """Extrai ID do vídeo da URL."""
        import re
        match = re.search(r'(?:v=|\/)([a-zA-Z0-9_-]{11})', url)
        if match:
            return match.group(1)
        raise ValueError(f"Não foi possível extrair video ID de: {url}")
    
    def _fetch_metadata(self, video_id: str) -> dict:
        """Busca metadados do vídeo."""
        # Exemplo: usar yt-dlp ou YouTube Data API
        return {
            "video_id": video_id,
            "processed_at": datetime.now().isoformat(),
            # Adicione mais campos conforme necessário
        }
    
    def _process_video(self, video_id: str, metadata: dict) -> dict:
        """Processa o vídeo com lógica customizada."""
        # Implemente sua lógica aqui:
        # - Gerar transcrições
        # - Análise de sentimento
        # - Extração de frames
        # - Upload para cloud storage
        # etc.
        
        return {
            "status": "completed",
            "video_id": video_id,
            "duration": metadata.get("duration"),
        }
    
    def _save_results(self, video_id: str, result: dict) -> None:
        """Salva resultados do processamento."""
        # Exemplo: salvar em banco de dados, S3, etc.
        output_file = f"results/{video_id}.json"
        os.makedirs("results", exist_ok=True)
        
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)
        
        logger.debug(f"Resultados salvos em: {output_file}")
    
    def get_stats(self) -> dict:
        """Retorna estatísticas do processamento."""
        return {
            "processed": self.processed_count,
            "errors": self.error_count,
            "success_rate": (
                self.processed_count / (self.processed_count + self.error_count)
                if (self.processed_count + self.error_count) > 0
                else 0
            )
        }


def main():
    """Função principal."""
    config = Config()
    orchestrator = Orchestrator(config)
    processor = VideoProcessor()
    
    logger.info("=" * 60)
    logger.info(f"🚀 Worker '{config.worker_name}' iniciado")
    logger.info(f"📊 Planilha: {config.spreadsheet_id}")
    logger.info("=" * 60)
    
    try:
        while True:
            # Processa próxima task
            if orchestrator.process_next_task(processor.process):
                stats = processor.get_stats()
                logger.info(
                    f"📊 Stats - Processados: {stats['processed']}, "
                    f"Erros: {stats['errors']}, "
                    f"Taxa de sucesso: {stats['success_rate']:.1%}"
                )
            
            # Heartbeat
            orchestrator.send_heartbeat()
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Parando worker...")
        stats = processor.get_stats()
        logger.info(f"📈 Estatísticas finais: {json.dumps(stats, indent=2)}")
        logger.info("✨ Finalizado")


if __name__ == "__main__":
    main()
