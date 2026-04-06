import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from shared.db import executeCustomQueryDF
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
        
        
    col_left, col_right = st.columns([1.5, 1])

    with col_left:
        if st.button("Select folder"):
            root = tk.Tk()
            root.withdraw()
            root.wm_attributes('-topmost', 1)
            st.session_state.path = filedialog.askdirectory(master=root)
            root.destroy()
            
        st.write(f"Folder to plot files of:{st.session_state.path}")
        
    with col_right:
        selected_dates = st.date_input("Select Timespan:",value=[datetime.date(2022, 1, 1), datetime.date(2023, 1, 1)])
        
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet')]
            if(len(files) == 0):
                st.error("Found no parquet files in this directory")
        except Exception as e:
            st.write("Path error")
            
        if(len(files) > 0):
            try:
                st.session_state.file = st.selectbox("Select the table you want to plot something of:", files, index=files.index(st.session_state.file))
            except Exception as e:
                st.session_state.file = st.selectbox("Select the table you want to plot something of:", files)
                
        if(st.session_state.file != ""):
            try:
                start_date, end_date = selected_dates
                file_path = os.path.join(st.session_state.path, st.session_state.file)
                queries = [f"SELECT CAST(CreationDate AS DATE) AS PostDate, COUNT(*) AS PostCount FROM '{file_path}' WHERE PostTypeId = 1 AND CreationDate >= '{start_date}' AND CreationDate <= '{end_date}' GROUP BY PostDate ORDER BY PostDate",
                           f"SELECT CAST(CreationDate AS DATE) AS PostDate, COUNT(*) AS PostCount FROM '{file_path}' WHERE PostTypeId = 2 AND CreationDate >= '{start_date}' AND CreationDate <= '{end_date}' GROUP BY PostDate ORDER BY PostDate"]
                st.session_state.query = st.selectbox("Select a predefined query:", queries)
                if(st.session_state.query != ""):
                    df = executeCustomQueryDF(st.session_state.query)
                    if df.empty:
                        st.warning("Query is empty")
                    else:
                        df["PostDate"] = pd.to_datetime(df["PostDate"])
                        fig = px.line(df, x="PostDate", y="PostCount")
                        for event_name, event_date in IMPORTANTDATES.items():
                            if start_date <= event_date <= end_date:
                                date_ms = pd.Timestamp(event_date).timestamp() * 1000
                                fig.add_vline(
                                    x=date_ms, 
                                    line_width=2, 
                                    line_dash="dash", 
                                    line_color="red",
                                    annotation_text=event_name, 
                                    annotation_position="top right")
                        st.plotly_chart(fig, use_container_width=True)
                        
                        with st.expander("Show raw data table"):
                            st.dataframe(df)

            except Exception as e:
                st.error(f"Something went wrong: {e}")