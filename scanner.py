#!/usr/bin/env python3
"""
Motor de escaneo 24/7 para XRPL inteligente mediante GitHub Actions.
Cumple con principios de robustez, manejo estricto de errores y persistencia atómica.
"""

import asyncio
import json
import logging
import os
import random
import sqlite3
import time
from typing import Any, Dict, List

from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models.requests.account_info import AccountInfo
from xrpl.wallet import Wallet

# Configuración de logging estructurado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("xrpl-scanner")

NODES: List[str] = [
    "https://s1.ripple.com:51234",
    "https://s2.ripple.com:51234",
    "https://xrplcluster.com",
]

DB_FILE: str = "xrpl_hits.db"
JSON_OUTPUT: str = "hits.json"
BATCH_SIZE: int = 50
CONCURRENCY_LIMIT: int = 10
REQUEST_TIMEOUT: float = 10.0


def init_db() -> None:
    """Inicializa la base de datos SQLite con WAL y configuración estricta."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hits (
                address TEXT PRIMARY KEY,
                seed TEXT NOT NULL,
                balance TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
            """
        )
        conn.commit()


async def check_wallet(node_url: str, wallet: Wallet, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
    """Verifica el saldo de una wallet en el nodo XRPL de forma asíncrona y acotada."""
    async with semaphore:
        client = AsyncJsonRpcClient(node_url)
        try:
            request = AccountInfo(account=wallet.address)
            response = await asyncio.wait_for(
                client.request(request), timeout=REQUEST_TIMEOUT
            )
            
            if response.is_successful():
                account_data = response.result.get("account_data", {})
                balance = account_data.get("Balance", "0")
                if int(balance) > 0:
                    logger.info(f"¡HIT detectado! Dirección: {wallet.address} | Saldo: {balance}")
                    return {
                        "hit": True,
                        "address": wallet.address,
                        "seed": wallet.seed,
                        "balance": str(balance),
                    }
        except asyncio.TimeoutError:
            logger.warning(f"Timeout al consultar el nodo {node_url} para la cuenta {wallet.address}")
        except Exception as e:
            logger.debug(f"Error consultando nodo {node_url}: {type(e).__name__} - {e}")
            
        return {"hit": False}


async def update_json_output() -> None:
    """Exporta los registros persistidos en SQLite hacia un archivo JSON plano."""
    if not os.path.exists(DB_FILE):
        return

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT address, seed, balance, timestamp FROM hits ORDER BY timestamp DESC")
        rows = cursor.fetchall()
        
    hits = [
        {
            "address": r[0],
            "seed": r[1],
            "balance": r[2],
            "timestamp": r[3],
        }
        for r in rows
    ]
    
    temp_output = f"{JSON_OUTPUT}.tmp"
    with open(temp_output, "w", encoding="utf-8") as f:
        json.dump(hits, f, indent=4, ensure_ascii=False)
    os.replace(temp_output, JSON_OUTPUT)


async def scan_cycle() -> None:
    """Ejecuta un ciclo completo de generación y validación de lotes de cuentas."""
    init_db()
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    logger.info(f"--- Iniciando ciclo de escaneo masivo: {time.ctime()} ---")
    
    tasks = [
        check_wallet(random.choice(NODES), Wallet.create(), semaphore)
        for _ in range(BATCH_SIZE)
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    hits_found = 0
    current_time = time.ctime()
    
    with sqlite3.connect(DB_FILE) as conn:
        for r in results:
            if r.get("hit"):
                hits_found += 1
                conn.execute(
                    "INSERT OR IGNORE INTO hits (address, seed, balance, timestamp) VALUES (?, ?, ?, ?)",
                    (r["address"], r["seed"], r["balance"], current_time),
                )
        conn.commit()

    await update_json_output()
    logger.info(f"✅ Ciclo finalizado. Hits nuevos en este lote: {hits_found}")


def main() -> None:
    """Punto de entrada principal del proceso continuo."""
    try:
        asyncio.run(scan_cycle())
    except Exception as e:
        logger.error(f"Error crítico en ciclo principal: {type(e).__name__} - {e}")
        raise


if __name__ == "__main__":
    main()
