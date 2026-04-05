from backend.castingTables import CastToCorrectTypes
from backend.parser import startParsing
from backend.complexity import calculateComplexity
from backend.describe import describeTables


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
                describeTables()
            case "3":
                CastToCorrectTypes()
            case "4":
                calculateComplexity()
            case _:
                print("Invalid input")

if __name__ == "__main__":
    main()