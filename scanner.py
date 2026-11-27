import asyncio
import json
import os
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.asyncio.wallet import generate_faucet_wallet
from xrpl.wallet import Wallet
from xrpl.account import get_account_info
from xrpl.models.requests.account_info import AccountInfo

# Configuración Async
CLIENT_URL = "https://s1.ripple.com:51234"
HITS_FILE = "hits.json"

async def check_wallet(client, wallet):
    try:
        request = AccountInfo(account=wallet.address)
        response = await client.request(request)
        if response.status == "success":
            balance = response.result.get("account_data", {}).get("Balance", 0)
            if int(balance) > 0:
                return {"address": wallet.address, "seed": wallet.seed, "balance": balance}
    except Exception:
        pass
    return None

async def scan():
    client = AsyncJsonRpcClient(CLIENT_URL)
    hits = []
    if os.path.exists(HITS_FILE):
        with open(HITS_FILE, 'r') as f:
            try:
                hits = json.load(f)
            except:
                hits = []
    
    # Generar batch de 100 carteras
    tasks = [check_wallet(client, Wallet.create()) for _ in range(100)]
    results = await asyncio.gather(*tasks)
    
    new_hits = [r for r in results if r is not None]
    if new_hits:
        hits.extend(new_hits)
        with open(HITS_FILE, 'w') as f:
            json.dump(hits, f, indent=2)
        print(f"Added {len(new_hits)} new hits.")

if __name__ == "__main__":
    asyncio.run(scan())
