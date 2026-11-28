import asyncio
import json
import os
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.wallet import Wallet
from xrpl.asyncio.ledger import get_account_info
from xrpl.asyncio.transaction import get_transaction_from_hash

# Nodo público de XRPL
JSON_RPC_URL = "https://s2.ripple.com:51234/"

async def scan_random_wallet():
    # Generar wallet aleatoria real
    wallet = Wallet.create()
    client = AsyncJsonRpcClient(JSON_RPC_URL)
    
    try:
        # Consultar información de la cuenta en el ledger
        response = await get_account_info(wallet.classic_address, client)
        balance = response.result["account_data"]["Balance"]
        
        if int(balance) > 0:
            return {
                "hit": True,
                "address": wallet.classic_address,
                "seed": wallet.seed,
                "balance": balance,
                "status": "FUNDED"
            }
    except:
        # Error 404 significa cuenta no creada en ledger (saldo 0)
        return {"hit": False}
    
    return {"hit": False}

async def main():
    print("Iniciando escaneo real...")
    result = await scan_random_wallet()
    
    # Persistir resultado
    with open("hits.json", "w") as f:
        json.dump({
            "updated_at": "2026-11-28T12:00:00Z",
            "total_hits": 1 if result["hit"] else 0,
            "data": result
        }, f, indent=2)

if __name__ == "__main__":
    asyncio.run(main())
