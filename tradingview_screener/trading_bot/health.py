"""
Simple health checks - not the reference architecture's async watchdog
with a heartbeat loop and a health_events table (that's built for a
long-running multi-symbol production deployment being monitored
externally). This is the synchronous equivalent scaled to what one
bot instance needs: something to run before starting, and something
deploy/healthcheck.py can call from cron or a container's healthcheck
directive.
"""
from __future__ import annotations
import logging

log = logging.getLogger("health")


def check_db(db_module) -> bool:
    try:
        db_module.get_open_trades()
        return True
    except Exception:
        log.exception("db health check failed")
        return False


def check_exchange(exchange) -> bool:
    try:
        exchange.get_mark_price("BTCUSDT")
        return True
    except Exception:
        log.exception("exchange health check failed")
        return False


def run_all(db_module, exchange) -> bool:
    db_ok = check_db(db_module)
    exchange_ok = check_exchange(exchange)
    if not db_ok:
        log.error("health check: database is not reachable")
    if not exchange_ok:
        log.error("health check: exchange is not reachable")
    return db_ok and exchange_ok
