import streamlit as st
import os
from shared.db import executeCustomQueryDF
from frontend.selector import selectFolder, selectboxWrapper


def analyzeBooksSite():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "file" not in st.session_state:
        st.session_state.file = ""
        
    if st.button("Select folder"):
        st.session_state.path = st.session_state.path = selectFolder()
        
        
    if(st.session_state.path != ""):
        try:
            files = [f for f in os.listdir(st.session_state.path) if f.lower().endswith('.parquet')]
        except Exception as e:
            st.write(f"Something went wrong: {e}")
            
        if(len(files) > 0):
            st.session_state.file = selectboxWrapper("Select a file:", files, st.session_state.file)
        else:
            st.error("Found no parquet files in this directory")

        if(st.session_state.file != ""):
            file_path = os.path.join(st.session_state.path, st.session_state.file)
            
            st.subheader(f"Description for `{st.session_state.file}`")
            
            try:
                description = executeCustomQueryDF(f"DESCRIBE SELECT * FROM '{file_path}'")
                st.dataframe(description, width='stretch')

                limit = st.number_input("Number of rows to preview:", min_value=1, max_value=1000, value=5, step=1)
                st.subheader(f"First {limit} rows of `{st.session_state.file}`")
                @st.cache_data
                def get_data(path):
                    return executeCustomQueryDF(f"SELECT * FROM '{path}' LIMIT 1000")
                full_df = get_data(file_path)
                st.dataframe(full_df.head(limit))

            except Exception as e:
                st.error(f"Failed to describe table!")