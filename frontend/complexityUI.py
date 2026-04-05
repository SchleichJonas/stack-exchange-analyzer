import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from backend.complexity import calculateComplexity
from shared.db import executeCustomQueryDF

def complexitySite():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "col" not in st.session_state:
        st.session_state.col = ""
        
    st.session_state.file = ""
        
    if st.button("Select folder"):
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        st.session_state.path = filedialog.askdirectory(master=root)
        root.destroy()
        
    st.write(f"Folder to parse:{st.session_state.path}")
        
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet') and "_typed" in f and not "_complexity" in f]
            if(len(files) == 0):
                st.error("Found no parquet files in this directory")
        except:
            st.write("Path error")
            
        if(len(files) > 0):
            try:
                st.session_state.file = st.selectbox("Select the table you want to compute the complexity of:", files, index=files.index(st.session_state.file))
            except:
                st.session_state.file = st.selectbox("Select the table you want to compute the complexity of:", files)
                
        if(st.session_state.file != ""):
            try:
                file_path = os.path.join(st.session_state.path, st.session_state.file)
                cols = executeCustomQueryDF(f"DESCRIBE SELECT * FROM '{file_path}'")['column_name']
                st.session_state.col = st.selectbox("Select the column for complexity compution", cols)
            except:
                st.error("Something went wrong")
                
    if st.button("Compute complexity"):
        if(st.session_state.path != "" and len(files) > 0 and st.session_state.file != ""):
            with st.spinner("Computing complexity..."):
                calculateComplexity(True, st.session_state.path, st.session_state.file, st.session_state.col)
                st.success("Casting complete.")
                st.subheader(f"First 10 rows of `{file_path[:-8]}_complexity.parquet`")
                preview_df = executeCustomQueryDF(f"SELECT * FROM '{file_path[:-8]}_complexity.parquet' LIMIT 10")
                st.dataframe(preview_df) 
        else:
            st.info("Please first select a directory with parquet files.")