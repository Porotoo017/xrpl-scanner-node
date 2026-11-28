import asyncio
import json
import os
from datetime import datetime
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.wallet import Wallet
from xrpl.asyncio.ledger import get_account_info

# Nodo público de XRPL
JSON_RPC_URL = "https://s2.ripple.com:51234/"

async def scan_random_wallet(client):
    wallet = Wallet.create()
    try:
        # La mayoría de las direcciones generadas al azar no existen en el ledger
        # y lanzarán una excepción. Eso significa saldo 0.
        response = await get_account_info(wallet.classic_address, client)
        balance = response.result["account_data"]["Balance"]
        return True, wallet.classic_address, wallet.seed, balance
    except Exception:
        return False, wallet.classic_address, None, "0"

async def main():
    client = AsyncJsonRpcClient(JSON_RPC_URL)
    results = []
    
    print(f"[{datetime.now()}] Iniciando ciclo de escaneo masivo...")
    
    # Escaneo en lote para optimizar I/O
    tasks = [scan_random_wallet(client) for _ in range(10)]
    outcomes = await asyncio.gather(*tasks)
    
    # Cargar hits previos si existen
    if os.path.exists("hits.json"):
        with open("hits.json", "r") as f:
            try:
                data = json.load(f)
                results = data.get("hits", [])
            except:
                results = []
                
    # Agregar nuevos hallazgos
    new_hit_found = False
    for is_funded, addr, seed, bal in outcomes:
        if is_funded:
            results.append({
                "address": addr,
                "seed": seed,
                "balance": bal,
                "timestamp": datetime.utcnow().isoformat()
            })
            new_hit_found = True
            
    with open("hits.json", "w") as f:
        json.dump({
            "updated_at": datetime.utcnow().isoformat(),
            "total_hits": len(results),
            "hits": results
        }, f, indent=2)
    
    print(f"[{datetime.now()}] Escaneo finalizado. Hits totales: {len(results)}")

if __name__ == "__main__":
    asyncio.run(main())
