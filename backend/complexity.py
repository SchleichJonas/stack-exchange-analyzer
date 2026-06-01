import os
from shared.db import get_connection
from shared.defines import IGNOREDDIRECTORIES

def calculateComplexity(gui = False, path = "", file = "", col = ""):
    """
    Calculates different complexity score of a column for a specific table

    Args:
        gui (bool, optional): Decides wheter the interface it GUI or command line. Defaults to False.
        path (str, optional): path to the directory containing parquet files, only used in GUI mode. Defaults to "".
        file (str, optional): path to the file the complexity is calculated of, only used in GUI mode. Defaults to "".
        col (str, optional): column to be used to compute the complexity, only used in GUI mode. Defaults to "".
    """    
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
        except Exception as e:
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
        
        typo_dataset = "./data/Mistake_to_Meaning.csv"
        
        con.execute(f"""
        COPY (
            WITH TypoData AS (SELECT list(lower(error)) AS typo_list FROM read_csv_auto('{typo_dataset}') WHERE length(error) >= 3 AND lower(error) NOT IN ('sin', 'cos', 'tan', 'cot', 'sec', 'csc', 'log', 'lim', 'max', 'min', 'sum', 'deg', 'rad', 'def', 'int', 'var', 'for', 'let', 'end', 'out', 'set'))
            SELECT 
                p.*,
                len(regexp_extract_all(p.{col}, '\\$\\$?[^\\$]+\\$\\$?')) AS TotalFormulaCount,
                len(regexp_extract_all(p.{col}, '\\$\\$?[^\\$]{{10,}}\\$\\$?')) AS LongFormulaCount,
                length(array_to_string(regexp_extract_all(p.{col}, '\\$\\$?[^\\$]+\\$\\$?'), '')) AS TotalFormulaLength,

                length(regexp_replace(p.{col}, '<[^>]+>', '', 'g')) AS CharCount,                
                len(regexp_extract_all(regexp_replace(p.{col}, '<[^>]+>', '', 'g'), '\\w+')) AS WordCount,
                
                (length(p.{col}) - length(replace(p.{col}, '—', ''))) AS EmDashCount,
                (length(p.{col}) - length(replace(p.{col}, '*', ''))) AS AsteriskCount,
                
                len(regexp_extract_all(p.{col}, '(?i)\\b(delv(e|es|ed|ing))\\b')) AS DelveCount,
                len(regexp_extract_all(p.{col}, '(?i)\\b(intricate(ly)?)\\b')) AS IntricateCount,
                len(regexp_extract_all(p.{col}, '(?i)\\b(underscore(s|d)?)\\b')) AS UnderscoreCount,
                len(regexp_extract_all(p.{col}, '(?i)\\b(crucial(ly)?)\\b')) AS CrucialCount,
                len(regexp_extract_all(p.{col}, '(?i)\\b(potential(ly)?)\\b')) AS PotentialCount,
                len(regexp_extract_all(p.{col}, '(?i)\\b(significant(ly)?)\\b')) AS SignificantCount,
                
                len(list_intersect(regexp_extract_all(lower(regexp_replace(p.{col}, '<[^>]+>|<code>.*?</code>', '', 'g')), '\\w+'), (SELECT typo_list FROM TypoData))) AS TypoCount
                
            FROM '{input_file}' AS p
        ) TO '{output_file}' (FORMAT PARQUET);
        """)