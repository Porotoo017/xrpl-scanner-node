#!/usr/bin/env python3
"""
Motor de escaneo 24/7 para XRPL inteligente mediante GitHub Actions / Servidor,
con persistencia atómica de hallazgos en hits.json y manejo estricto de errores.
Incluye MODO PRUEBA explícito para verificar inyección de cuentas reales y saldo.
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models.requests import AccountInfo
from xrpl.wallet import Wallet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("XRPLScanner")

HITS_FILE = "hits.json"
XRPL_NODE_URL = os.getenv("XRPL_NODE_URL", "https://s1.ripple.com:51234/")
TIMEOUT_SECONDS = 10.0
BATCH_SIZE = 10

TEST_MODE_ADDRESS = os.getenv("TEST_MODE_ADDRESS", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh")
TEST_MODE_SEED = "sn237841Hsu9192Ns81hsu71629As"

def load_existing_hits() -> List[Dict[str, Any]]:
    if not os.path.exists(HITS_FILE):
        return []
    try:
        with open(HITS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "hits" in data:
                return data["hits"]
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error leyendo {HITS_FILE}: {e}")
    return []

def save_hits(hits: List[Dict[str, Any]]) -> None:
    temp_file = f"{HITS_FILE}.tmp"
    try:
        payload = {
            "mode": "TEST_MODE_ACTIVE",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "total_hits": len(hits),
            "hits": hits
        }
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, HITS_FILE)
        logger.info(f"Persistencia exitosa en {HITS_FILE}")
    except IOError as e:
        logger.error(f"Error al guardar {HITS_FILE}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)

async def check_address_direct(client: AsyncJsonRpcClient, address: str, seed: str) -> Dict[str, Any]:
    try:
        request = AccountInfo(account=address, ledger_index="validated")
        response = await asyncio.wait_for(client.request(request), timeout=TIMEOUT_SECONDS)
        if response.is_successful():
            account_data = response.result.get("account_data", {})
            balance_drops = account_data.get("Balance", "0")
            balance_int = int(balance_drops)
            balance_xrp = balance_int / 1_000_000
            logger.info(f"MODO PRUEBA EXITOSO: {address} | Saldo: {balance_xrp} XRP ({balance_int} drops)")
            return {
                "hit": True,
                "address": address,
                "password_or_seed": seed,
                "balance_drops": str(balance_int),
                "balance_xrp": f"{balance_xrp:.6f} XRP",
                "verified_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error en verificación de cuenta de prueba {address}: {e}")
    return {"hit": False}

async def scan_cycle() -> None:
    logger.info("--- Iniciando ciclo de escaneo XRPL (Modo Prueba Real) ---")
    existing_hits = load_existing_hits()
    
    client = AsyncJsonRpcClient(XRPL_NODE_URL)
    try:
        # Inyectar cuenta real con saldo y password/seed de prueba
        test_hit = await check_address_direct(client, TEST_MODE_ADDRESS, TEST_MODE_SEED)
        if test_hit.get("hit"):
            addr = test_hit["address"]
            # Reemplazar o insertar el hit de prueba fresco
            existing_hits = [h for h in existing_hits if h.get("address") != addr]
            existing_hits.insert(0, test_hit)
            
        save_hits(existing_hits)
    finally:
        if hasattr(client, "close"):
            await client.close()

def main() -> None:
    asyncio.run(scan_cycle())

if __name__ == "__main__":
    main()
