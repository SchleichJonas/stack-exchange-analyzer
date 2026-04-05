import os
from shared.db import get_connection
from shared.defines import IGNOREDDIRECTORIES

def calculateComplexity(gui = False, path = "", file = "", col = ""):
    con = get_connection()
    action = "y"
    exists = False
    if(gui == False):
        dirs = [f for f in os.listdir() if not os.path.isfile(os.path.join(f)) and not f in IGNOREDDIRECTORIES]
        for i, dir in enumerate(dirs):
            print(f"{i} {dir}")
            
        folder = input()
        files = [f for f in os.listdir(dirs[int(folder)]) if os.path.isfile(os.path.join(dirs[int(folder)], f))]
        for i, file in enumerate(files):
            print(f"{i} {file}")
            
        file = input()
        folder = dirs[int(folder)]
        file = files[int(file)]
        cols = con.execute(f"DESCRIBE SELECT * FROM '{folder}/{file}'").fetchdf()
        print("Select which column should be used for complexity computation")
        for i, col in enumerate(cols['column_name']):
            print(f"{i} {col}")
        col = input()
        try:
            col = cols['column_name'][int(col)]
        except:
            return
        
        exists = False
        if os.path.isfile(f"{folder}/{file[:-8]}_complexity.parquet"):
            exists = True
            print("file already exists with complexity calculation, want to redo it? y/n:")
            action = input()
    
    else:
        folder = path
        
    if (exists == False) or (action == "y"):
        input_file = f"{folder}/{file}"
        output_file = f"{folder}/{file[:-8]}_complexity.parquet" 
        
        con.execute(f"""
        COPY (
            SELECT 
                *,
                len(regexp_extract_all({col}, '\\$\\$?[^\\$]{{5,}}\\$\\$?')) AS SimpleFormulaCount,
                len(regexp_extract_all({col}, '\\$\\$?[^\\$]{{10,}}\\$\\$?')) AS LongFormulaCount
            FROM '{input_file}'
        ) TO '{output_file}' (FORMAT PARQUET);
        """)