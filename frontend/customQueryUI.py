import streamlit as st
import os
from shared.db import executeCustomQueryDF


def customQuerySite():
    if "path" not in st.session_state:
        st.session_state.path = ""
    if "file" not in st.session_state:
        st.session_state.file = ""
    file_path = os.path.join(st.session_state.path, st.session_state.file)
    if "custom_query" not in st.session_state:
        st.session_state.custom_query = f"SELECT DISTINCT TagName FROM '{file_path}'"
        
    st.session_state.custom_query = st.text_input("Enter you custom sql query", st.session_state.custom_query)
    
    if st.button("Submit"):
        try:
            result = executeCustomQueryDF(st.session_state.custom_query)
            with st.expander(st.session_state.custom_query, expanded=True):
                st.dataframe(result)
            
        except Exception as e:
            print(f"Something went wrong: {e}")