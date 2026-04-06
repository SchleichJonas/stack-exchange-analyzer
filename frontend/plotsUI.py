import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from shared.db import executeCustomQueryDF
from frontend.selector import selectFolder, selectboxWrapper
import datetime
import plotly.express as px
from shared.defines import IMPORTANTDATES
import pandas as pd



def plotsSite():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "col" not in st.session_state:
        st.session_state.col = ""
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "file" not in st.session_state:
        st.session_state.file = ""
    if "dates" not in st.session_state:
        st.session_state.dates = True
    if "rolling" not in st.session_state:
        st.session_state.rolling = 30
        
        
    col_left, col_middle_left, col_middle_right, col_right = st.columns([1.5, 0.2, 0.2, 1])

    with col_left:
        if st.button("Select folder"):
            st.session_state.path = selectFolder()
            
        st.write(f"Folder to plot files of:{st.session_state.path}")
    
    with col_middle_left:
        st.session_state.rolling = st.number_input("Rolling average range", min_value=1, max_value=1000, value=30, step=1)
    
    with col_middle_right:
        st.session_state.dates = st.toggle("Display release dates", True)
        
    with col_right:
        selected_dates = st.date_input("Select Timespan:",value=[datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)])
        
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet')]
        except Exception as e:
            st.write(f"Something went wrong: {e}")
            
        if(len(files) > 0):
            st.session_state.file = selectboxWrapper("Select the table you want to plot something of:", files, st.session_state.file)
        else:
            st.error("Found no parquet files in this directory")
                
        if(st.session_state.file != ""):
            try:
                start_date, end_date = selected_dates
                file_path = os.path.join(st.session_state.path, st.session_state.file)
                queries = [f"SELECT CAST(CreationDate AS DATE) AS PostDate, COUNT(*) AS PostCount FROM '{file_path}' WHERE PostTypeId = 1 GROUP BY PostDate ORDER BY PostDate",
                           f"SELECT CAST(CreationDate AS DATE) AS PostDate, COUNT(*) AS PostCount FROM '{file_path}' WHERE PostTypeId = 2 GROUP BY PostDate ORDER BY PostDate"]
                st.session_state.query = selectboxWrapper("Select a predefined query:", queries, "")
                if(st.session_state.query != ""):
                    df = executeCustomQueryDF(st.session_state.query)
                    if df.empty:
                        st.warning("Query is empty")
                    else:
                        df["PostDate"] = pd.to_datetime(df["PostDate"])
                        df[f"{st.session_state.rolling} day trend"] = df["PostCount"].rolling(window=st.session_state.rolling, min_periods=1).mean()
                        fig = px.line(df, x="PostDate", y=["PostCount", f"{st.session_state.rolling} day trend"])
                        fig.update_xaxes(range=[pd.to_datetime(start_date), pd.to_datetime(end_date)])
                        fig.data[1].line.color = 'red'
                        fig.data[1].line.width = 3
                        for event_name, event_date in IMPORTANTDATES.items():
                            if(st.session_state.dates):
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