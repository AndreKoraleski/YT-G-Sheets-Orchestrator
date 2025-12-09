import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

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

# Estado global para rate limiting
_last_request_time: float = 0.0
_base_rate_limit: float = 1.0
_jitter_max_seconds: float = 0.5
_active_workers: int = 1  # Número de workers ativos (atualizado periodicamente)


def configure_rate_limiting(rate_limit_seconds: float, jitter_max_seconds: float) -> None:
    """
    Configura parâmetros de rate limiting para todas as operações do gateway.

    Cada worker usa sua própria service account, então o rate limit é constante.
    O jitter aumenta proporcionalmente ao número de workers para dessincronizar.

    Args:
        rate_limit_seconds: Tempo entre requisições em segundos.
        jitter_max_seconds: Jitter base (multiplicado por número de workers - 1).
    """
    global _base_rate_limit, _jitter_max_seconds
    _base_rate_limit = rate_limit_seconds
    _jitter_max_seconds = jitter_max_seconds
    logger.info(
        f"Rate limiting configurado: {rate_limit_seconds}s + jitter proporcional aos workers"
    )


def update_active_workers(count: int) -> None:
    """
    Atualiza o número de workers ativos para cálculo dinâmico de rate limiting.

    Args:
        count: Número atual de workers ativos.
    """
    global _active_workers
    old_count = _active_workers
    _active_workers = max(1, count)  # Mínimo de 1
    if old_count != _active_workers:
        logger.info(f"Workers ativos atualizado: {old_count} -> {_active_workers}")


def _apply_rate_limit() -> None:
    """
    Aplica rate limiting com jitter proporcional ao número de workers.

    Rate limit base é constante (cada worker tem sua própria service account).
    Jitter aumenta com mais workers para dessincronizar e evitar colisões:
    - 1 worker: sem jitter (0s)
    - 2 workers: até 0.5s de jitter
    - 5 workers: até 2.0s de jitter
    """
    global _last_request_time

    now = time.time()
    elapsed = now - _last_request_time

    # Jitter proporcional ao número de workers para dessincronizar
    if _active_workers == 1:
        jitter = 0.0
    else:
        effective_jitter = _jitter_max_seconds * (_active_workers - 1)
        jitter = random.uniform(0, effective_jitter)

    # Calcula delay necessário (rate limit constante + jitter)
    base_delay = max(0, _base_rate_limit - elapsed)
    total_delay = base_delay + jitter

    if total_delay > 0:
        if _active_workers == 1:
            logger.debug(f"Rate limiting: aguardando {total_delay:.3f}s (sem jitter)")
        else:
            logger.debug(
                f"Rate limiting: aguardando {total_delay:.3f}s "
                f"(base: {_base_rate_limit:.3f}s, jitter: {jitter:.3f}s "
                f"[até {effective_jitter:.3f}s para {_active_workers} workers])"
            )
        time.sleep(total_delay)

    _last_request_time = time.time()


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
            # Aplica rate limiting antes de cada tentativa
            _apply_rate_limit()
            return function()

        except NON_RETRYABLE_EXCEPTIONS:
            # Erros lógicos/esperados - não retry
            raise

        except Exception as e:
            exception = e
            if attempt == tries:
                logger.error(
                    "Todas as tentativas falharam após %d tentativas: %s",
                    tries,
                    str(e),
                    exc_info=True,
                )
                break

            logger.warning(
                "Tentativa %d falhou com erro: %s. Retentando em %.2f segundos...",
                attempt,
                str(e),
                wait,
            )
            time.sleep(wait)
            wait *= backoff

    assert exception is not None
    raise exception
