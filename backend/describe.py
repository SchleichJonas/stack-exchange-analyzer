import os
from shared.db import get_connection
from shared.defines import IGNOREDDIRECTORIES

def describeTables():
    con = get_connection()
    dirs = [f for f in os.listdir() if not os.path.isfile(os.path.join(f)) and not f in IGNOREDDIRECTORIES]
    for i, dir in enumerate(dirs):
        print(f"{i} {dir}")
        
    folder = input()
    try:
        folder = int(folder)
    except:
        return
    
    dir = dirs[folder]
    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) and f.endswith(".parquet")]
    try:
        for file in files:
            print(f"DESCRIBE SELECT * FROM '{dir}/{file}':")
            result = con.execute(f"DESCRIBE SELECT * FROM '{dir}/{file}'").fetchdf()
            print(result)
    except:
            print("ERROR")
            return