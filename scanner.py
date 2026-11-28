import asyncio
import json
import os
from datetime import datetime
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests import AccountInfo

# Nodos oficiales públicos de Ripple para balanceo de carga nativo
JSON_RPC_URLS = [
    "https://s1.ripple.com:51234/",
    "https://s2.ripple.com:51234/"
]

BATCH_SIZE = 500  # Límite máximo absoluto por ejecución sin saturar límites de rate-limit de red

async def scan_random_wallet(client, semaphore):
    async with semaphore:
        wallet = Wallet.create()
        try:
            req = AccountInfo(
                account=wallet.classic_address,
                ledger_index="validated"
            )
            response = await client.request(req)
            if response.is_successful():
                balance = response.result["account_data"]["Balance"]
                return True, wallet.classic_address, wallet.seed, balance
            return False, wallet.classic_address, None, "0"
        except Exception:
            return False, wallet.classic_address, None, "0"

async def main():
    # Alternar nodo para máxima velocidad
    url = JSON_RPC_URLS[datetime.utcnow().second % len(JSON_RPC_URLS)]
    client = AsyncJsonRpcClient(url)
    
    # 50 conexiones concurrentes optimizadas para I/O asíncrono masivo
    semaphore = asyncio.Semaphore(50)
    results = []
    
    print(f"[{datetime.now()}] ESCANEO MÁXIMO ACTIVADO: Consultando lote de {BATCH_SIZE} carteras vía {url}...")
    
    tasks = [scan_random_wallet(client, semaphore) for _ in range(BATCH_SIZE)]
    outcomes = await asyncio.gather(*tasks)
    
    if os.path.exists("hits.json"):
        with open("hits.json", "r") as f:
            try:
                data = json.load(f)
                results = data.get("hits", [])
            except:
                results = []
                
    new_hits = 0
    for is_funded, addr, seed, bal in outcomes:
        if is_funded:
            new_hits += 1
            results.append({
                "address": addr,
                "seed": seed,
                "balance": bal,
                "timestamp": datetime.utcnow().isoformat()
            })
            
    with open("hits.json", "w") as f:
        json.dump({
            "updated_at": datetime.utcnow().isoformat(),
            "total_hits": len(results),
            "hits": results
        }, f, indent=2)
    
    print(f"[{datetime.now()}] Ciclo completado. Lote: {BATCH_SIZE} | Encontrados nuevos: {new_hits} | Total acumulado: {len(results)}")

if __name__ == "__main__":
    asyncio.run(main())
