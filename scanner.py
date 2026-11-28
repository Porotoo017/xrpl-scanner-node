import asyncio
import json
import os
import sqlite3
import time
import random
from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models.requests.account_info import AccountInfo

# Configuración masiva de nodos XRPL
NODES = [
    "https://s1.ripple.com:51234",
    "https://s2.ripple.com:51234",
    "https://xrplcluster.com"
]
DB_FILE = "xrpl_hits.db"
JSON_OUTPUT = "hits.json"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''CREATE TABLE IF NOT EXISTS hits 
                    (address TEXT PRIMARY KEY, seed TEXT, balance TEXT, timestamp DATETIME)''')
    conn.commit()
    conn.close()

async def check_wallet(node_url, wallet, semaphore):
    async with semaphore:
        client = AsyncJsonRpcClient(node_url)
        try:
            request = AccountInfo(account=wallet.address)
            response = await client.request(request)
            if response.status == "success":
                balance = response.result.get("account_data", {}).get("Balance", 0)
                if int(balance) > 0:
                    return {"hit": True, "address": wallet.address, "seed": wallet.seed, "balance": balance}
        except Exception:
            pass
        return {"hit": False}

async def update_json_output():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM hits")
    rows = cursor.fetchall()
    hits = [{"address": r[0], "seed": r[1], "balance": r[2], "timestamp": r[3]} for r in rows]
    with open(JSON_OUTPUT, "w") as f:
        json.dump(hits, f, indent=4)
    conn.close()

async def scan():
    init_db()
    conn = sqlite3.connect(DB_FILE)
    batch_size = 50
    semaphore = asyncio.Semaphore(10)
    
    print(f"--- Iniciando ciclo de escaneo masivo: {time.ctime()} ---")
    
    tasks = [check_wallet(random.choice(NODES), Wallet.create(), semaphore) for _ in range(batch_size)]
    results = await asyncio.gather(*tasks)
    
    hits_found = 0
    for r in results:
        if r.get("hit"):
            hits_found += 1
            ts = time.ctime()
            conn.execute("INSERT OR IGNORE INTO hits VALUES (?, ?, ?, ?)", 
                         (r['address'], r['seed'], r['balance'], ts))
            print(f"🚨 NUEVO HIT: {r['address']} | Saldo: {r['balance']}")
            
    conn.commit()
    conn.close()
    await update_json_output()
    print(f"✅ Ciclo finalizado. Total hits acumulados: {hits_found}")

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(scan())
        except Exception as e:
            print(f"Error en ciclo: {e}")
        time.sleep(2)
