import os
import json
from xrpl.wallet import Wallet
from xrpl.clients import JsonRpcClient
from xrpl.account import get_account_info

# Configuración
client = JsonRpcClient("https://s1.ripple.com:51234")
HITS_FILE = "hits.json"

def scan():
    hits = []
    if os.path.exists(HITS_FILE):
        with open(HITS_FILE, 'r') as f:
            hits = json.load(f)
    
    # Escaneo inteligente: generamos, verificamos saldo y guardamos si > 0
    wallet = Wallet.create()
    try:
        response = get_account_info(wallet.address, client)
        if response:
            balance = response.get("account_data", {}).get("Balance", 0)
            if int(balance) > 0:
                print(f"HIT FOUND: {wallet.address}")
                hits.append({"address": wallet.address, "seed": wallet.seed, "balance": balance})
                with open(HITS_FILE, 'w') as f:
                    json.dump(hits, f)
    except Exception as e:
        # Direcciones sin activar arrojan error de cuenta no encontrada (es normal)
        pass

if __name__ == "__main__":
    for _ in range(50): # Escaneo por batch en cada corrida
        scan()
