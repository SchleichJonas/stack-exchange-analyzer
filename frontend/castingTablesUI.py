import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from backend.castingTables import CastToCorrectTypes
from shared.db import get_connection


def castingTablesSite():
    st.warning("This will create new '_typed.parquet' files.")
    if "path" not in st.session_state:
        st.session_state.path = ""
    if st.button("Select folder"):
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        st.session_state.path = filedialog.askdirectory(master=root)
        root.destroy()
            

    st.write(f"Folder to convert:{st.session_state.path}")
    
    
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet') and not "_typed" in f]
            if(len(files) == 0):
                st.error("Found no parquet files in this directory")
        except:
            st.write("Path error")
            
    con = get_connection()
    
    if(st.session_state.path != "" and len(files) > 0):                
            with st.expander("Select files to cast", expanded=True):
                selected_map = {}
                for f in files:
                    selected_map[f] = st.checkbox(f"Cast {f}", value=True)

            files_to_cast = [f for f, checked in selected_map.items() if checked]
    
    if st.button("Run Type Casting"):
        if(st.session_state.path != "" and len(files) > 0):
            with st.spinner("Casting..."):
                CastToCorrectTypes(True, st.session_state.path, files_to_cast)
                st.success("Casting complete.")
        else:
            st.info("Please first select a directory with parquet files.")