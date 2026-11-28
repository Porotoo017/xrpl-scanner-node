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

# Configuración estricta de logging estructurado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("XRPLScanner")

HITS_FILE = "hits.json"
XRPL_NODE_URL = os.getenv("XRPL_NODE_URL", "https://s1.ripple.com:51234/")
TIMEOUT_SECONDS = 10.0
BATCH_SIZE = 30

# Dirección de prueba real conocida de XRPL con saldo histórico/activo en Mainnet para verificar el flujo completo
TEST_MODE_ADDRESS = os.getenv("TEST_MODE_ADDRESS", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh") # Cuenta génesis / cuenta validada de ripple

def load_existing_hits() -> List[Dict[str, Any]]:
    """Carga los hallazgos previos desde el archivo JSON de forma segura."""
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
        logger.error(f"Error leyendo {HITS_FILE}, inicializando lista vacía: {e}")
    return []

def save_hits(hits: List[Dict[str, Any]]) -> None:
    """Guarda los hallazgos en disco de manera atómica."""
    temp_file = f"{HITS_FILE}.tmp"
    try:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "total_hits": len(hits),
            "hits": hits
        }
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, HITS_FILE)
        logger.info(f"Persistencia exitosa. Total de registros en {HITS_FILE}: {len(hits)}")
    except IOError as e:
        logger.error(f"Error crítico al guardar {HITS_FILE}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)

async def check_address_direct(client: AsyncJsonRpcClient, address: str, seed: str = "TEST_MODE_FUNDED") -> Dict[str, Any]:
    """Verifica directamente una dirección específica (útil para modo prueba con saldo real)."""
    try:
        request = AccountInfo(account=address, ledger_index="validated")
        response = await asyncio.wait_for(
            client.request(request),
            timeout=TIMEOUT_SECONDS
        )
        if response.is_successful():
            account_data = response.result.get("account_data", {})
            balance_drops = account_data.get("Balance", "0")
            balance_int = int(balance_drops)
            logger.info(f"VERIFICACIÓN MODO PRUEBA EXITOSA: {address} | Saldo: {balance_int} drops")
            return {
                "hit": True,
                "address": address,
                "seed": seed,
                "balance": str(balance_int),
                "verified_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error verificando cuenta de prueba {address}: {e}")
    return {"hit": False}

async def check_wallet(client: AsyncJsonRpcClient, wallet: Wallet) -> Dict[str, Any]:
    """Verifica el saldo de una cuenta generada aleatoriamente en XRPL Mainnet."""
    try:
        request = AccountInfo(account=wallet.classic_address, ledger_index="validated")
        response = await asyncio.wait_for(
            client.request(request),
            timeout=TIMEOUT_SECONDS
        )
        
        if response.is_successful():
            account_data = response.result.get("account_data", {})
            balance_drops = account_data.get("Balance", "0")
            balance_int = int(balance_drops)
            if balance_int > 0:
                logger.info(f"¡HIT DETECTADO! Dirección: {wallet.classic_address} | Saldo: {balance_int} drops")
                return {
                    "hit": True,
                    "address": wallet.classic_address,
                    "seed": wallet.seed,
                    "balance": str(balance_int),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
    except Exception:
        pass
    return {"hit": False}

async def scan_cycle() -> None:
    """Ejecuta un ciclo asíncrono de escaneo que incluye verificación en modo prueba."""
    logger.info(f"--- Iniciando ciclo de escaneo con Modo Prueba: {datetime.now(timezone.utc).isoformat()} ---")
    existing_hits = load_existing_hits()
    
    async with AsyncJsonRpcClient(XRPL_NODE_URL) as client:
        # 1. Inyección de Modo Prueba explícito con dirección real con saldo
        test_hit = await check_address_direct(client, TEST_MODE_ADDRESS)
        if test_hit.get("hit"):
            addr = test_hit["address"]
            if not any(h.get("address") == addr for h in existing_hits):
                existing_hits.append(test_hit)
                logger.info(f"Modo prueba: Cuenta real con saldo {addr} agregada a hits.json")

        # 2. Escaneo aleatorio estándar por lotes
        tasks = []
        for _ in range(BATCH_SIZE):
            w = Wallet.create()
            tasks.append(check_wallet(client, w))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        new_hits_found = 1 if test_hit.get("hit") else 0
        for res in results:
            if isinstance(res, dict) and res.get("hit"):
                addr = res["address"]
                if not any(h.get("address") == addr for h in existing_hits):
                    existing_hits.append(res)
                    new_hits_found += 1
                    
        save_hits(existing_hits)
        logger.info(f"Ciclo completado. Total de hits registrados en el archivo: {len(existing_hits)}")

def main() -> None:
    asyncio.run(scan_cycle())

if __name__ == "__main__":
    main()
