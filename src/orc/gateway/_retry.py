import time
import logging

from typing import Callable, TypeVar
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound


logger = logging.getLogger(__name__)

ReturnType = TypeVar("ReturnType")

# Exceções que NÃO devem passar por retry (erros lógicos/esperados)
NON_RETRYABLE_EXCEPTIONS = (
    WorksheetNotFound,
    SpreadsheetNotFound,
    ValueError,
    KeyError,
    TypeError,
)


def retry(
        function: Callable[[], ReturnType],
        tries: int = 5,
        delay: float = 1.0,
        backoff: float = 2.0,
) -> ReturnType:
    """
    Tenta executar uma função várias vezes com um atraso exponencial entre as tentativas em caso de falha.
    
    Exceções lógicas/esperadas (WorksheetNotFound, SpreadsheetNotFound, ValueError, etc.) 
    NÃO passam por retry e são lançadas imediatamente.
    
    Apenas erros transiêntes de rede/API são retryados.

    Args:
        function (Callable): A função a ser executada.
        tries (int): Número máximo de tentativas. Padrão é 5.
        delay (float): Atraso inicial entre as tentativas em segundos. Padrão é 1.0.
        backoff (float): Fator de multiplicação para o atraso após cada falha.
    Returns:
        O resultado da função executada, se bem-sucedida.
    """
    exception: Exception | None = None
    wait = delay

    for attempt in range(1, tries + 1):
        try:
            return function()
        
        except NON_RETRYABLE_EXCEPTIONS:
            # Erros lógicos/esperados - não retry
            raise
            
        except Exception as e:
            exception = e
            if attempt == tries:
                logger.error("Todas as tentativas falharam após %d tentativas: %s", tries, str(e), exc_info=True)
                break

            logger.warning("Tentativa %d falhou com erro: %s. Retentando em %.2f segundos...", attempt, str(e), wait)
            time.sleep(wait)
            wait *= backoff

    assert exception is not None
    raise exception
