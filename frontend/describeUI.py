import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from shared.db import executeCustomQueryDF


def describeSite():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "file" not in st.session_state:
        st.session_state.file = ""
        
    if st.button("Select folder"):
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes('-topmost', 1)
        st.session_state.path = filedialog.askdirectory(master=root)
        root.destroy()
        
        
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet')]
            if(len(files) == 0):
                st.error("Found no parquet files in this directory")
        except:
            st.write("Path error")
            
        if(len(files) > 0):
            try:
                st.session_state.file = st.selectbox("Select a table to describe:", files, index=files.index(st.session_state.file))
            except:
                st.session_state.file = st.selectbox("Select a table to describe:", files)

            if st.session_state.file:
                file_path = os.path.join(st.session_state.path, st.session_state.file)
                
                st.subheader(f"Description for `{st.session_state.file}`")
                
                try:
                    description = executeCustomQueryDF(f"DESCRIBE SELECT * FROM '{file_path}'")
                    st.dataframe(description, width='stretch')

                    limit = st.number_input("Number of rows to preview:", min_value=1, max_value=1000, value=5, step=1)
                    st.subheader(f"First {limit} rows of `{st.session_state.file}`")
                    @st.cache_data # This tells Streamlit to remember the result
                    def get_data(path):
                        return executeCustomQueryDF(f"SELECT * FROM '{path}' LIMIT 1000")
                    #preview_df = executeCustomQueryDF(f"SELECT * FROM '{file_path}' LIMIT {limit}")
                    full_df = get_data(file_path)
                    st.dataframe(full_df.head(limit))

                except Exception as e:
                    st.error(f"Failed to describe table!")