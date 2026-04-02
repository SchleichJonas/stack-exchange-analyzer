import duckdb
from castingTables import CastToCorrectTypes
from parser import startParsing

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




def main():
    while True:
        print("Please select on what you would like to do:")
        print("1 Parse XML files to parquet files")
        print("2 Describe all tables")
        print("3 Cast tables to correct types (creates new files called [tableName]_typed.parquet)")
        print("4 Exit")
        action = input()
        match action:
            case "1":
                startParsing()
            case "2":
                Describe()
            case "3":
                CastToCorrectTypes(con)
            case "4":
                return
            case _:
                print("Invalid input")

if __name__ == "__main__":
    main()