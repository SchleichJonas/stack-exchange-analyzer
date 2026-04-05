import os
from shared.defines import IGNOREDDIRECTORIES
from shared.db import get_connection

def selection():
    """
    Provides the command line interface to select the tables to parse to its correct types

    Returns:
        string: directory of the parquet files
    """    
    print("Enter directory you want to cast")
    dirs = [f for f in os.listdir() if not os.path.isfile(os.path.join(f)) and not f in IGNOREDDIRECTORIES]
    for i, dir in enumerate(dirs):
        print(f"{i} {dir}")
        
    folder = input()
    try:
        folder = int(folder)
    except Exception as e:
        return
    
    dir = dirs[folder]
    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f)) and f.endswith(".parquet")]
    files_exist = False
    for file in files:
        if "typed" in file:
            files_exist = True

    if files_exist:
        print("There are already typed files in this directory, do you want to redo it? y/n")
        if(input() == "n"):
            return "",[]
        
    return dir
    
def CastToCorrectTypes(gui=False, path = "", files = []):
    """
    Casts the columns of tables from a directory to its correct types

    Args:
        gui (bool, optional): Decides wheter the interface it GUI or command line. Defaults to False.
        path (str, optional): path of the directory, only used in GUI mode. Defaults to "".
        files (list, optional): files to cast to its correct types, only used in GUI mode. Defaults to [].
    """    
    con = get_connection()
    dir = ""
    if(gui == False):
        dir = selection()
        if(dir == ""):
            return
    else:
        dir = path

    if((os.path.isfile(f"{dir}/Badges.parquet") and path == "") or (path != "" and "Badges.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(TagBased AS BOOLEAN) AS TagBased,
                CAST(Class AS INTEGER) AS Class,
                CAST(Date AS TIMESTAMP) AS Date,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(Id AS INTEGER) AS Id,
                Name
            FROM '{dir}/Badges.parquet'
        ) TO '{dir}/Badges_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/Comments.parquet") and path == "") or (path != "" and "Comments.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(Score AS INTEGER) AS Score,
                CAST(Id AS INTEGER) AS Id,
                Text,
                ContentLicense,
                UserDisplayName
            FROM '{dir}/Comments.parquet'
        ) TO '{dir}/Comments_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/PostHistory.parquet") and path == "") or (path != "" and "PostHistory.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(PostHistoryTypeId AS INTEGER) AS PostHistoryTypeId,
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(RevisionGUID AS UUID) AS RevisionGUID,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(Id AS INTEGER) AS Id,
                Text,
                Comment,
                ContentLicense,
                UserDisplayName
            FROM '{dir}/PostHistory.parquet'
        ) TO '{dir}/PostHistory_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/PostLinks.parquet") and path == "") or (path != "" and "PostLinks.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(RelatedPostId AS INTEGER) AS RelatedPostId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(LinkTypeId AS INTEGER) AS LinkTypeId,
                CAST(Id AS INTEGER) AS Id
            FROM '{dir}/PostLinks.parquet'
        ) TO '{dir}/PostLinks_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/Posts.parquet") and path == "") or (path != "" and "Posts.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(ViewCount AS INTEGER) AS ViewCount,
                CAST(LastEditDate AS TIMESTAMP) AS LastEditDate,
                CAST(PostTypeId AS INTEGER) AS PostTypeId,
                CAST(AnswerCount AS INTEGER) AS AnswerCount,
                CAST(OwnerUserId AS INTEGER) AS OwnerUserId,
                CAST(ClosedDate AS TIMESTAMP) AS ClosedDate,
                CAST(AcceptedAnswerId AS INTEGER) AS AcceptedAnswerId,
                CAST(ParentId AS INTEGER) AS ParentId,
                CAST(Id AS INTEGER) AS Id,
                CAST(LastActivityDate AS TIMESTAMP) AS LastActivityDate,
                CAST(CommentCount AS INTEGER) AS CommentCount,
                CAST(LastEditorUserId AS INTEGER) AS LastEditorUserId,
                CAST(CommunityOwnedDate AS TIMESTAMP) AS CommunityOwnedDate,
                CAST(Score AS INTEGER) AS Score,
                Tags,
                ContentLicense,
                Title,
                Body,
                OwnerDisplayName,
                LastEditorDisplayName
            FROM '{dir}/Posts.parquet'
        ) TO '{dir}/Posts_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/Tags.parquet") and path == "") or (path != "" and "Tags.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(WikiPostId AS INTEGER) AS WikiPostId,
                CAST(Count AS INTEGER) AS Count,
                CAST(Id AS INTEGER) AS Id,
                CAST(ExcerptPostId AS INTEGER) AS ExcerptPostId,
                TagName
            FROM '{dir}/Tags.parquet'
        ) TO '{dir}/Tags_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/Users.parquet") and path == "") or (path != "" and "Users.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(LastAccessDate AS TIMESTAMP) AS LastAccessDate,
                CAST(UpVotes AS INTEGER) AS UpVotes,
                CAST(Views AS INTEGER) AS Views,
                CAST(AccountId AS INTEGER) AS AccountId,
                CAST(Reputation AS INTEGER) AS Reputation,
                CAST(Id AS INTEGER) AS Id,
                CAST(DownVotes AS INTEGER) AS DownVotes,
                WebsiteUrl,
                Location,
                DisplayName,
                AboutMe
            FROM '{dir}/Users.parquet'
        ) TO '{dir}/Users_typed.parquet' (FORMAT PARQUET);
        """)
    
    if((os.path.isfile(f"{dir}/Votes.parquet") and path == "") or (path != "" and "Votes.parquet" in files)):
        con.execute(f"""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(BountyAmount AS INTEGER) AS BountyAmount,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(Id AS INTEGER) AS Id,
                CAST(VoteTypeId AS INTEGER) AS VoteTypeId
            FROM '{dir}/Votes.parquet'
        ) TO '{dir}/Votes_typed.parquet' (FORMAT PARQUET);
        """)
    