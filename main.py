import duckdb
from castingTables import CastToCorrectTypes
from parser import startParsing
import os
from defines import IGNOREDDIRECTORIES

con = duckdb.connect()


def get_connection():
    return con

def executeCustomQueryDF(query):
    return con.execute(query).fetch_df()

def Describe():
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

def test():
    #result = con.execute(f"SELECT * FROM 'mathoverflow/Posts_typed_complexity.parquet' limit 10").fetchdf()
    result = con.execute(f"SELECT * FROM 'mathoverflow/Posts_typed_complexity.parquet' limit 10").fetchdf()
    print(result['Body'].iloc[0])
    for x in result:
        print(result[x])
    
def calculateComplexity(gui = False, path = "", file = "", col = ""):
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


def main():
    while True:
        print("Please select on what you would like to do:")
        print("0 Exit")
        print("1 Parse XML files to parquet files")
        print("2 Describe all tables")
        print("3 Cast tables to correct types (creates new files called [tableName]_typed.parquet)")
        print("4 Calculate complexity of a file")
        
        action = input()
        match action:
            case "0":
                return
            case "1":
                startParsing()
            case "2":
                Describe()
            case "3":
                CastToCorrectTypes(con)
            case "4":
                calculateComplexity()
                test()
            case _:
                print("Invalid input")

if __name__ == "__main__":
    main()