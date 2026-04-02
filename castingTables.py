import os

def alreadyTyped(filename):
    print(f"{filename} already exists, do you want to redo the casting?")
    print("1 yes")
    print("2 no")
    print("3 yes to all")
    print("4 no to all")
    action = input()
    return action
    
def CastToCorrectTypes(con):
    print("Casting all columns to its correct type")
    action = 0;
    if os.path.isfile("mathoverflow/Badges_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/Badges_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(TagBased AS BOOLEAN) AS TagBased,
                CAST(Class AS INTEGER) AS Class,
                CAST(Date AS TIMESTAMP) AS Date,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(Id AS INTEGER) AS Id,
                Name
            FROM 'mathoverflow/Badges.parquet'
        ) TO 'mathoverflow/Badges_typed.parquet' (FORMAT PARQUET);
        """)
    
    if os.path.isfile("mathoverflow/Comments_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/Comments_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathoverflow/Comments.parquet'
        ) TO 'mathoverflow/Comments_typed.parquet' (FORMAT PARQUET);
        """)
    
    if os.path.isfile("mathoverflow/PostHistory_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/PostHistory_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathoverflow/PostHistory.parquet'
        ) TO 'mathoverflow/PostHistory_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathoverflow/PostLinks_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/PostLinks_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(RelatedPostId AS INTEGER) AS RelatedPostId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(LinkTypeId AS INTEGER) AS LinkTypeId,
                CAST(Id AS INTEGER) AS Id
            FROM 'mathoverflow/PostLinks.parquet'
        ) TO 'mathoverflow/PostLinks_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathoverflow/Posts_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/Posts_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathoverflow/Posts.parquet'
        ) TO 'mathoverflow/Posts_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathoverflow/Tags_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/Tags_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(WikiPostId AS INTEGER) AS WikiPostId,
                CAST(Count AS INTEGER) AS Count,
                CAST(Id AS INTEGER) AS Id,
                CAST(ExcerptPostId AS INTEGER) AS ExcerptPostId,
                TagName
            FROM 'mathoverflow/Tags.parquet'
        ) TO 'mathoverflow/Tags_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathoverflow/Users_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/Users_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathoverflow/Users.parquet'
        ) TO 'mathoverflow/Users_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathoverflow/Votes_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathoverflow/Votes_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(BountyAmount AS INTEGER) AS BountyAmount,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(Id AS INTEGER) AS Id,
                CAST(VoteTypeId AS INTEGER) AS VoteTypeId
            FROM 'mathoverflow/Votes.parquet'
        ) TO 'mathoverflow/Votes_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/Badges_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/Badges_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(TagBased AS BOOLEAN) AS TagBased,
                CAST(Class AS INTEGER) AS Class,
                CAST(Date AS TIMESTAMP) AS Date,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(Id AS INTEGER) AS Id,
                Name
            FROM 'mathstackexchange/Badges.parquet'
        ) TO 'mathstackexchange/Badges_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/Comments_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/Comments_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathstackexchange/Comments.parquet'
        ) TO 'mathstackexchange/Comments_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/PostHistory_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/PostHistory_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathstackexchange/PostHistory.parquet'
        ) TO 'mathstackexchange/PostHistory_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/PostLinks_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/PostLinks_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(RelatedPostId AS INTEGER) AS RelatedPostId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(LinkTypeId AS INTEGER) AS LinkTypeId,
                CAST(Id AS INTEGER) AS Id
            FROM 'mathstackexchange/PostLinks.parquet'
        ) TO 'mathstackexchange/PostLinks_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/Posts_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/Posts_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathstackexchange/Posts.parquet'
        ) TO 'mathstackexchange/Posts_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/Tags_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/Tags_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(WikiPostId AS INTEGER) AS WikiPostId,
                CAST(Count AS INTEGER) AS Count,
                CAST(Id AS INTEGER) AS Id,
                CAST(ExcerptPostId AS INTEGER) AS ExcerptPostId,
                TagName
            FROM 'mathstackexchange/Tags.parquet'
        ) TO 'mathstackexchange/Tags_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/Users_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/Users_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
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
            FROM 'mathstackexchange/Users.parquet'
        ) TO 'mathstackexchange/Users_typed.parquet' (FORMAT PARQUET);
        """)
    
    
    if os.path.isfile("mathstackexchange/Votes_typed.parquet"):
        if(action != "3") and (action != "4"):
            action = alreadyTyped("mathstackexchange/Votes_typed")
    else:
        action = "1"
    if(action == "1") or (action == "3"):
        con.execute("""
        COPY (
            SELECT 
                CAST(CreationDate AS TIMESTAMP) AS CreationDate,
                CAST(BountyAmount AS INTEGER) AS BountyAmount,
                CAST(UserId AS INTEGER) AS UserId,
                CAST(PostId AS INTEGER) AS PostId,
                CAST(Id AS INTEGER) AS Id,
                CAST(VoteTypeId AS INTEGER) AS VoteTypeId
            FROM 'mathstackexchange/Votes.parquet'
        ) TO 'mathstackexchange/Votes_typed.parquet' (FORMAT PARQUET);
        """)