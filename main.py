import duckdb
from castingTables import CastToCorrectTypes
from parser import startParsing
import os

con = duckdb.connect()

mathoverflowFiles = ['mathoverflow/Badges.parquet', 'mathoverflow/Comments.parquet', 'mathoverflow/PostHistory.parquet',
                     'mathoverflow/PostLinks.parquet', 'mathoverflow/Posts.parquet', 'mathoverflow/Tags.parquet',
                     'mathoverflow/Users.parquet', 'mathoverflow/Votes.parquet']

mathStackExchangeflowFiles = ['mathstackexchange/Badges.parquet', 'mathstackexchange/Comments.parquet', 'mathstackexchange/PostHistory.parquet',
                     'mathstackexchange/PostLinks.parquet', 'mathstackexchange/Posts.parquet', 'mathstackexchange/Tags.parquet',
                     'mathstackexchange/Users.parquet', 'mathstackexchange/Votes.parquet']

mathoverflowFiles_typed = ['mathoverflow/Badges_typed.parquet', 'mathoverflow/Comments_typed.parquet', 'mathoverflow/PostHistory_typed.parquet',
                     'mathoverflow/PostLinks_typed.parquet', 'mathoverflow/Posts_typed.parquet', 'mathoverflow/Tags_typed.parquet',
                     'mathoverflow/Users_typed.parquet', 'mathoverflow/Votes_typed.parquet']

mathStackExchangeflowFiles_typed = ['mathstackexchange/Badges_typed.parquet', 'mathstackexchange/Comments_typed.parquet', 'mathstackexchange/PostHistory_typed.parquet',
                     'mathstackexchange/PostLinks_typed.parquet', 'mathstackexchange/Posts_typed.parquet', 'mathstackexchange/Tags_typed.parquet',
                     'mathstackexchange/Users_typed.parquet', 'mathstackexchange/Votes_typed.parquet']


def Describe():
    try:
        for file in mathoverflowFiles_typed:
            print(f"DESCRIBE SELECT * FROM '{file}':")
            result = con.execute(f"DESCRIBE SELECT * FROM '{file}'").fetchdf()
            print(result)
    except:
        try:
            for file in mathoverflowFiles:
                print(f"DESCRIBE SELECT * FROM '{file}':")
                result = con.execute(f"DESCRIBE SELECT * FROM '{file}'").fetchdf()
                print(result)
        except:
            print("You first need to cast the XML files to parquet and put them in a folder called mathoverflow in this directory")
                   
    try:
        for file in mathStackExchangeflowFiles_typed:
            print(f"DESCRIBE SELECT * FROM '{file}':")
            result = con.execute(f"DESCRIBE SELECT * FROM '{file}'").fetchdf()
            print(result)
    except:
        try:
            for file in mathStackExchangeflowFiles:
                print(f"DESCRIBE SELECT * FROM '{file}':")
                result = con.execute(f"DESCRIBE SELECT * FROM '{file}'").fetchdf()
                print(result)
        except:
            print("You first need to cast the XML files to parquet and put them in a folder called mathstackexchange in this directory")

def test():
    #result = con.execute(f"SELECT * FROM 'mathoverflow/Posts_typed_complexity.parquet' limit 10").fetchdf()
    result = con.execute(f"SELECT * FROM 'mathoverflow/Posts_typed_complexity.parquet' limit 10").fetchdf()
    print(result['Body'].iloc[0])
    for x in result:
        print(result[x])
    
def calculateComplexity(folder, file):
    exists = False
    if os.path.isfile(f"{folder}/{file[:-8]}_complexity.parquet"):
        exists = True
        print("file already exists with complexity calculation, want to redo it? y/n:")
        action = input()
    
    if (exists == False) or (action == "y"):
        input_file = f"{folder}/{file}"
        output_file = f"{folder}/{file[:-8]}_complexity.parquet" 
        
        con.execute(f"""
        COPY (
            SELECT 
                *,
                len(regexp_extract_all(Body, '\\$\\$?[^\\$]{{5,}}\\$\\$?')) AS SimpleFormulaCount,
                len(regexp_extract_all(Body, '\\$\\$?[^\\$]{{10,}}\\$\\$?')) AS LongFormulaCount
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
                dirs = [f for f in os.listdir() if not os.path.isfile(os.path.join(f))]
                for i, dir in enumerate(dirs):
                    print(f"{i} {dir}")
                    
                folder = input()
                files = [f for f in os.listdir(dirs[int(folder)]) if os.path.isfile(os.path.join(dirs[int(folder)], f))]
                for i, file in enumerate(files):
                    print(f"{i} {file}")
                    
                file = input()
                calculateComplexity(dirs[int(folder)], files[int(file)])
                test()
            case _:
                print("Invalid input")

if __name__ == "__main__":
    main()