import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from backend.parser import startParsing


def parserSite():
    """
    Provides the webpage to parse XML file to parquet files
    """  
    if "path" not in st.session_state:
        st.session_state.path = ""
    if st.button("Select folder"):
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        st.session_state.path = filedialog.askdirectory(master=root)
        root.destroy()
            
            
    
    st.write(f"Folder to parse:{st.session_state.path}")
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.xml')]
            if(len(files) == 0):
                st.error("No XML files found in this path!")
            else:
                st.write(f"Found {len(files)} XML files.")
        except Exception as e:
            st.error(f"Path not found in {st.session_state.path}!")
        
    if st.button("Start Parsing"):
        if(st.session_state.path != "" and len(files) > 0):
            with st.spinner("Parsing..."):
                startParsing(st.session_state.path)
                st.success("Parsing complete!")