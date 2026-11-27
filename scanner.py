import asyncio
import json
import os
import sqlite3
import time
import random
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests.account_info import AccountInfo

# Configuración
NODES = [
    "https://s1.ripple.com:51234",
    "https://s2.ripple.com:51234",
    "https://xrplcluster.com"
]
DB_FILE = "xrpl_hits.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''CREATE TABLE IF NOT EXISTS hits 
                    (address TEXT PRIMARY KEY, seed TEXT, balance TEXT, timestamp DATETIME)''')
    conn.commit()
    conn.close()

async def check_wallet(node_url, wallet):
    client = AsyncJsonRpcClient(node_url)
    start = time.perf_counter()
    try:
        request = AccountInfo(account=wallet.address)
        response = await client.request(request)
        latency = time.perf_counter() - start
        
        if response.status == "success":
            balance = response.result.get("account_data", {}).get("Balance", 0)
            return {"hit": True, "address": wallet.address, "seed": wallet.seed, "balance": balance, "latency": latency}
    except Exception:
        pass
    return {"hit": False, "latency": time.perf_counter() - start}

async def scan():
    init_db()
    conn = sqlite3.connect(DB_FILE)
    
    batch_size = 50
    start_total = time.perf_counter()
    
    tasks = []
    for _ in range(batch_size):
        node = random.choice(NODES)
        tasks.append(check_wallet(node, Wallet.create()))
    
    results = await asyncio.gather(*tasks)
    
    total_time = time.perf_counter() - start_total
    hits_found = 0
    
    for r in results:
        if r.get("hit"):
            hits_found += 1
            conn.execute("INSERT OR IGNORE INTO hits VALUES (?, ?, ?, ?)", 
                         (r['address'], r['seed'], r['balance'], time.ctime()))
    
    conn.commit()
    conn.close()
    
    # Telemetría
    print(f"Batch completado: {batch_size} carteras en {total_time:.2f}s")
    print(f"Throughput: {batch_size/total_time:.2f} rps")
    print(f"Hits encontrados: {hits_found}")

if __name__ == "__main__":
    asyncio.run(scan())
