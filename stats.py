import duckdb

con = duckdb.connect()

mathoverflowFiles = ['mathoverflow/Badges.parquet', 'mathoverflow/Comments.parquet', 'mathoverflow/PostHistory.parquet',
                     'mathoverflow/PostLinks.parquet', 'mathoverflow/Posts.parquet', 'mathoverflow/Tags.parquet',
                     'mathoverflow/Users.parquet', 'mathoverflow/Votes.parquet']

mathStackExchangeflowFiles = ['mathstackexchange/Badges.parquet', 'mathstackexchange/Comments.parquet', 'mathstackexchange/PostHistory.parquet',
                     'mathstackexchange/PostLinks.parquet', 'mathstackexchange/Posts.parquet', 'mathstackexchange/Tags.parquet',
                     'mathstackexchange/Users.parquet', 'mathstackexchange/Votes.parquet']



for file in mathoverflowFiles:
    print(f"DESCRIBE SELECT * FROM '{file}':")
    result = con.execute(f"DESCRIBE SELECT * FROM '{file}'").fetchdf()
    print(result)
 
#We shoud cast everything we can to the appropriate type since all cols are currently VARCHAR
#con.execute("""
#CREATE OR REPLACE VIEW posts_clean AS
#SELECT
#    CAST(Id AS BIGINT) AS Id,
#    CAST(OwnerUserId AS BIGINT) AS OwnerUserId,
#    CAST(ParentId AS BIGINT) AS ParentId,
#    CAST(PostTypeId AS INTEGER) AS PostTypeId,
#    CAST(Score AS INTEGER) AS Score,
#    CAST(AnswerCount AS INTEGER) AS AnswerCount,
#    CAST(CommentCount AS INTEGER) AS CommentCount,
#    CreationDate::TIMESTAMP AS CreationDate,
#    Title
#FROM 'mathoverflow/Posts.parquet'
#""")
    
    

#for file in mathStackExchangeflowFiles:
#    print(f"DESCRIBE SELECT * FROM '{file}':")
#    result = con.execute(f"DESCRIBE SELECT * FROM '{file}'").fetchdf()
#    print(result)