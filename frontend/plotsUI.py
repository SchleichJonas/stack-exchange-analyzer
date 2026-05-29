import streamlit as st
import os
from shared.db import executeCustomQueryDF
from frontend.selector import selectFolder, selectboxWrapper
import datetime
import plotly.express as px
from shared.defines import IMPORTANTDATES
import pandas as pd

def eventSelector():
    with st.expander("Select release dates to be included in the plot", expanded=False):
        selected_map = {}
        for entry in IMPORTANTDATES.keys():
            selected_map[entry] = st.checkbox(f"{entry} - {IMPORTANTDATES.get(entry)}", value=False)
        return selected_map


def plotDatapoint(column, plots):
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "col" not in st.session_state:
        st.session_state.col = ""
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "file" not in st.session_state:
        st.session_state.file = "Posts.parquet"
    if "dates" not in st.session_state:
        st.session_state.dates = True
    if "rolling" not in st.session_state:
        st.session_state.rolling = 30

    st.session_state.rolling = st.number_input("Rolling average range", min_value=1, max_value=1000, value=30, step=1)
    selected_map = eventSelector()
    
    if(st.session_state.path != ""):       
        if(st.session_state.file != ""):
            try:
                start_date, end_date = st.session_state.dates
                if(st.session_state.query != ""):
                    df = executeCustomQueryDF(st.session_state.query)
                    if df.empty:
                        st.warning("Query is empty")
                    else:
                        df["PostDate"] = pd.to_datetime(df["PostDate"])
                        df[f"{st.session_state.rolling} day trend"] = df[column].rolling(window=st.session_state.rolling, min_periods=1).mean()
                        fig = px.line(df, x="PostDate", 
                                      y=[column, 
                                      f"{st.session_state.rolling} day trend"], 
                                      title=plots[st.session_state.plot_index])
                        fig.data[1].line.color = 'red'
                        fig.data[1].line.width = 3
                        fig.update_xaxes(range=[pd.to_datetime(start_date), pd.to_datetime(end_date)])
                        fig.update_layout(uirevision=str(st.session_state.dates))
                        st.session_state.last_dates = st.session_state.dates
                        for event_name, event_date in IMPORTANTDATES.items():
                            if(selected_map[event_name]):
                                date_ms = pd.Timestamp(event_date).timestamp() * 1000
                                fig.add_vline(
                                    x=date_ms, 
                                    line_width=2, 
                                    line_dash="dash", 
                                    line_color="red",
                                    annotation_text=event_name, 
                                    annotation_position="top right")
                        st.plotly_chart(fig, width='stretch')
                        
                        with st.expander("Show raw data table"):
                            st.dataframe(df)

            except Exception as e:
                st.error(f"Something went wrong: {e}")


def plotTags(plots):
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "col" not in st.session_state:
        st.session_state.col = ""
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "file" not in st.session_state:
        st.session_state.file = "Posts.parquet"
    if "rolling" not in st.session_state:
        st.session_state.rolling = 30
       
    selected_map = eventSelector()
    
    if(st.session_state.path != ""):
        if(st.session_state.file != ""):
            try:
                start_date, end_date = st.session_state.dates
                if(st.session_state.query != ""):
                    df = executeCustomQueryDF(st.session_state.query)
                    if df.empty:
                        st.warning("Query is empty")
                    else:
                        fig = px.line(df, x="PostDate", 
                                      y="TagCount", 
                                      color="TagName",
                                      title=plots[st.session_state.plot_index])
                        fig.update_traces(line_width=2)
                        fig.update_xaxes(range=[pd.to_datetime(start_date), pd.to_datetime(end_date)])
                        fig.update_layout(uirevision=str(st.session_state.dates))
                        st.session_state.last_dates = st.session_state.dates
                        for event_name, event_date in IMPORTANTDATES.items():
                            if(selected_map[event_name]):
                                date_ms = pd.Timestamp(event_date).timestamp() * 1000
                                fig.add_vline(
                                    x=date_ms, 
                                    line_width=2, 
                                    line_dash="dash", 
                                    line_color="red",
                                    annotation_text=event_name, 
                                    annotation_position="top right")
                        st.plotly_chart(fig, width='stretch')
                        
                        with st.expander("Show raw data table"):
                            st.dataframe(df)

            except Exception as e:
                st.error(f"Something went wrong: {e}")



def scatterPlot():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "col" not in st.session_state:
        st.session_state.col = ""
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "file" not in st.session_state:
        st.session_state.file = "Posts.parquet"
    if "dates" not in st.session_state:
        st.session_state.dates = True
    if "xaxis" not in st.session_state:
        st.session_state.xaxis = ""
    if "yaxis" not in st.session_state:
        st.session_state.yaxis = ""


    file_path = os.path.join(st.session_state.path, st.session_state.file)
    cols = executeCustomQueryDF(f"DESCRIBE SELECT * FROM '{file_path}'")['column_name']
    st.session_state.xaxis = selectboxWrapper("Select the column for the x-axis", cols, "Score")
    st.session_state.yaxis = selectboxWrapper("Select the column for the y-axis", cols, "ViewCount")
    df = executeCustomQueryDF(f"SELECT {st.session_state.xaxis}, {st.session_state.yaxis} FROM '{file_path}' USING SAMPLE 10000")
    
    try:
        fig = px.scatter(
        df,
        x=st.session_state.xaxis,
        y=st.session_state.yaxis,
        title=f"Correlation of {st.session_state.xaxis} and {st.session_state.yaxis}",
        opacity=0.4,
        hover_data=[st.session_state.xaxis, st.session_state.yaxis],
        trendline="ols", 
        trendline_color_override="red"
        )
        results = px.get_trendline_results(fig)
        model = results.px_fit_results.iloc[0]
        slope = round(model.params[1], 4)
        r_squared = round(model.rsquared, 4)
        fig.data[1].name = f"Slope: {slope}, R^2: {r_squared})"
        fig.data[1].showlegend = True
        st.plotly_chart(fig, width='stretch')

    except Exception as e:
        st.error(f"Something went wrong: {e}")

def plotsSite():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "col" not in st.session_state:
        st.session_state.col = ""
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "file" not in st.session_state:
        st.session_state.file = "Posts.parquet"
    if "dates" not in st.session_state:
        st.session_state.dates = ""
    if "rolling" not in st.session_state:
        st.session_state.rolling = 30
    if "plot" not in st.session_state:
        st.session_state.plot_index = 0
    if "tag_count" not in st.session_state:
        st.session_state.tag_count = 10
        
        
    if st.button("Select folder"):
        st.session_state.path = selectFolder()
        
    try:
        files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet')]
    except Exception as e:
        st.write(f"Something went wrong: {e}")
        
    st.session_state.dates = st.date_input("Select Timespan:",value=[datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)], min_value=datetime.date(2009, 1, 1), max_value=datetime.date(2026, 1, 1))
        
    if(len(files) > 0):
        st.session_state.file = selectboxWrapper("Select the table you want to plot something of:", files, st.session_state.file)
        
    
    filepath = os.path.join(st.session_state.path, st.session_state.file)
    queries = [
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, COUNT(*) AS PostCount FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, COUNT(*) AS PostCount FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        
        f"WITH TopTags AS (SELECT tag FROM (SELECT unnest(string_split(trim(Tags, '<>'), '><')) AS tag FROM '{filepath}' WHERE PostTypeId = 1 AND Tags IS NOT NULL) GROUP BY tag ORDER BY COUNT(*) DESC LIMIT 10),MonthlyTags AS (SELECT date_trunc('month', CreationDate) AS PostMonth,unnest(string_split(trim(Tags, '<>'), '><')) AS tag FROM '{filepath}'WHERE PostTypeId = 1 AND Tags IS NOT NULL)SELECT CAST(PostMonth AS DATE) AS PostDate, tag AS TagName,COUNT(*) AS TagCount FROM MonthlyTags WHERE tag IN (SELECT tag FROM TopTags) GROUP BY PostDate, TagName ORDER BY PostDate",
        
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, AVG(WordCount) AS AverageWordCount FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, AVG(WordCount) AS AverageWordCount FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(EmDashCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedEmDashRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(EmDashCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedEmDashRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(EmDashCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedEmDashRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(AsteriskCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedAsteriskRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(AsteriskCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedAsteriskRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(AsteriskCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedAsteriskRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(DelveCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedDelveRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(DelveCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedDelveRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(DelveCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedDelveRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(IntricateCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedIntricateRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(IntricateCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedIntricateRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(IntricateCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedIntricateRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(UnderscoreCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedUnderscoreRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(UnderscoreCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedUnderscoreRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(UnderscoreCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedUnderscoreRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(DelveCount + IntricateCount + UnderscoreCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedKeyLLMRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(DelveCount + IntricateCount + UnderscoreCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedKeyLLMRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(DelveCount + IntricateCount + UnderscoreCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedKeyLLMRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",

        f"SELECT CAST(CreationDate AS DATE) AS PostDate, AVG(TypoCount) AS AverageTypoCount FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, AVG(TypoCount) AS AverageTypoCount FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, AVG(TypoCount) AS AverageTypoCount FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",
        
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(TypoCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedTypoRate FROM '{filepath}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(TypoCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedTypoRate FROM '{filepath}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate",
        f"SELECT CAST(CreationDate AS DATE) AS PostDate, (SUM(TypoCount) * 1000.0) / NULLIF(SUM(WordCount), 0) AS NormalizedTypoRate FROM '{filepath}' GROUP BY PostDate ORDER BY PostDate",
        
        f"PLACEHOLDER"
    ]

    plots = [
        f"Questions per day",                                               # 0
        f"Answers per day",                                                 # 1
        f"Top {st.session_state.tag_count} tags usage over time",           # 2
        f"Average Word count of questions over time",                       # 3
        f"Average Word count of answers over time",                         # 4
        f"Normalized EM dash count of questions over time",                 # 5
        f"Normalized EM dash count of answers over time",                   # 6
        f"Normalized EM dash count of posts over time",                     # 7
        f"Normalized Asterisk count of questions over time",                # 8
        f"Normalized Asterisk count of answers over time",                  # 9
        f"Normalized Asterisk count of posts over time",                    # 10
        f"Normalized delve count of questions over time",                   # 11
        f"Normalized delve count of answers over time",                     # 12
        f"Normalized delve count of posts over time",                       # 13
        f"Normalized intricate count of questions over time",               # 14
        f"Normalized intricate count of answers over time",                 # 15
        f"Normalized intricate count of posts over time",                   # 16
        f"Normalized underscore count of questions over time",              # 17
        f"Normalized underscore count of answers over time",                # 18
        f"Normalized underscore count of posts over time",                  # 19
        f"Normalized combined LLM keywords count of questions over time",   # 20
        f"Normalized combined LLM keywords count of answers over time",     # 21
        f"Normalized combined LLM keywords count of posts over time",       # 22
        f"Average typo count of questions over time",                       # 23
        f"Average typo count of answers over time",                         # 24
        f"Average typo count over time",                                    # 25
        f"Normalized typo count of questions over time",                    # 26
        f"Normalized typo count of answers over time",                      # 27
        f"Normalized typo count over time",                                 # 28
        f"Scatter plot",                                                    # 29
    ]

    selected_plot = selectboxWrapper("Select a predefined plot:", plots, "")
    st.session_state.plot_index = plots.index(selected_plot)
    st.session_state.query = queries[st.session_state.plot_index]

    # Perfect index mapping block
    if(st.session_state.plot_index == 0 or st.session_state.plot_index == 1):
        plotDatapoint("PostCount", plots)
    elif(st.session_state.plot_index == 2):
        plotTags(plots)
    elif(st.session_state.plot_index == 3 or st.session_state.plot_index == 4):
        plotDatapoint("AverageWordCount", plots)
    elif(st.session_state.plot_index == 5 or st.session_state.plot_index == 6 or st.session_state.plot_index == 7):
        plotDatapoint("NormalizedEmDashRate", plots)
    elif(st.session_state.plot_index == 8 or st.session_state.plot_index == 9 or st.session_state.plot_index == 10):
        plotDatapoint("NormalizedAsteriskRate", plots)
    elif(st.session_state.plot_index == 11 or st.session_state.plot_index == 12 or st.session_state.plot_index == 13):
        plotDatapoint("NormalizedDelveRate", plots)
    elif(st.session_state.plot_index == 14 or st.session_state.plot_index == 15 or st.session_state.plot_index == 16):
        plotDatapoint("NormalizedIntricateRate", plots)
    elif(st.session_state.plot_index == 17 or st.session_state.plot_index == 18 or st.session_state.plot_index == 19):
        plotDatapoint("NormalizedUnderscoreRate", plots)
    elif(st.session_state.plot_index == 20 or st.session_state.plot_index == 21 or st.session_state.plot_index == 22):
        plotDatapoint("NormalizedKeyLLMRate", plots)
    elif(st.session_state.plot_index == 23 or st.session_state.plot_index == 24 or st.session_state.plot_index == 25):
        plotDatapoint("AverageTypoCount", plots)
    elif(st.session_state.plot_index == 26 or st.session_state.plot_index == 27 or st.session_state.plot_index == 28):
        plotDatapoint("NormalizedTypoRate", plots)
    elif(st.session_state.plot_index == 29):
        scatterPlot()
        