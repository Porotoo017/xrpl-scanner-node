import asyncio
import json
import os
import sqlite3
import time
import random
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests.account_info import AccountInfo

# Configuración masiva de nodos XRPL oficiales y clusters públicos
NODES = [
    "https://s1.ripple.com:51234",
    "https://s2.ripple.com:51234",
    "https://xrplcluster.com",
    "https://s.altnet.rippletest.net:51234"
]
DB_FILE = "xrpl_hits.db"
LOG_FILE = "hits_report.txt"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''CREATE TABLE IF NOT EXISTS hits 
                    (address TEXT PRIMARY KEY, seed TEXT, balance TEXT, timestamp DATETIME)''')
    conn.commit()
    conn.close()

async def check_wallet(node_url, wallet, semaphore):
    async with semaphore:
        client = AsyncJsonRpcClient(node_url)
        start = time.perf_counter()
        try:
            request = AccountInfo(account=wallet.address)
            response = await client.request(request)
            latency = time.perf_counter() - start
            
            if response.status == "success":
                balance = response.result.get("account_data", {}).get("Balance", 0)
                # Si el balance es mayor a 0, es un hit relevante
                if int(balance) > 0:
                    return {"hit": True, "address": wallet.address, "seed": wallet.seed, "balance": balance, "latency": latency}
        except Exception:
            pass
        return {"hit": False, "latency": time.perf_counter() - start}

async def scan():
    init_db()
    conn = sqlite3.connect(DB_FILE)
    
    # MÁXIMA POTENCIA: Lotes masivos de 100 carteras para mantener estabilidad en render
    batch_size = 100
    semaphore = asyncio.Semaphore(20)
    
    print(f"--- Iniciando nuevo ciclo de escaneo masivo ---")
    
    tasks = []
    for _ in range(batch_size):
        node = random.choice(NODES)
        wallet = Wallet.create()
        tasks.append(check_wallet(node, wallet, semaphore))
    
    results = await asyncio.gather(*tasks)
    
    hits_found = 0
    for r in results:
        if r.get("hit"):
            hits_found += 1
            timestamp = time.ctime()
            conn.execute("INSERT OR IGNORE INTO hits VALUES (?, ?, ?, ?)", 
                         (r['address'], r['seed'], r['balance'], timestamp))
            
            # Reporte en archivo de texto para que lo puedas descargar o leer fácilmente
            with open(LOG_FILE, "a") as f:
                f.write(f"[{timestamp}] HIT! {r['address']} | Saldo: {r['balance']} | Seed: {r['seed']}\n")
            
            print(f"🚨 HIT DETECTADO: {r['address']} | Saldo: {r['balance']}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Ciclo finalizado. Hits encontrados: {hits_found}")

if __name__ == "__main__":
    # Bucle infinito para que corra 24/7 en Render
    while True:
        try:
            asyncio.run(scan())
        except Exception as e:
            print(f"Error en ciclo: {e}")
        time.sleep(5) # Pequeña pausa entre ciclos
