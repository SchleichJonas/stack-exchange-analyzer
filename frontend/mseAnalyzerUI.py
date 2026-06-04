import streamlit as st
import os
from shared.db import executeCustomQueryDF
from frontend.selector import selectFolder, selectboxWrapper
from backend.mse_analyzer import main


def mseAnalyzerSite():
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
            
            st.subheader(f"MSE for `{st.session_state.file}`")
            
            try:
                figures = main(file_path)

                for title, fig in figures.items():
                    st.subheader(title.replace("_", " ").title())
                    st.pyplot(fig)

            except Exception as e:
                st.error(f"ERROR:{e}")