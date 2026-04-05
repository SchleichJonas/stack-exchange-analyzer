import streamlit as st
import tkinter as tk
from tkinter import filedialog
import os
from parser import startParsing
from main import executeCustomQueryDF, get_connection, calculateComplexity
from castingTables import CastToCorrectTypes


def main():
    st.set_page_config(page_title="Stack exchange analyzer", layout="wide")

    st.sidebar.title("Actions")
    action = st.sidebar.radio(
        "Please select what you would like to do:",
        [
            "Parse XML to Parquet", 
            "Describe Tables", 
            "Cast Tables to Types", 
            "Calculate Complexity"
        ]
    )

    st.title(action)

    if action == "Parse XML to Parquet":
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
            except:
                st.error(f"Path not found in {st.session_state.path}!")
            
        if st.button("Start Parsing"):
            if(st.session_state.path != "" and len(files) > 0):
                with st.spinner("Parsing..."):
                    startParsing(st.session_state.path)
                    st.success("Parsing complete!")



    elif action == "Describe Tables":
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
                        st.dataframe(description, use_container_width=True)

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

    elif action == "Cast Tables to Types":
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
                    CastToCorrectTypes(con, True, st.session_state.path, files_to_cast)
                    st.success("Casting complete.")
            else:
                st.info("Please first select a directory with parquet files.")
            
        

    elif action == "Calculate Complexity":
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
                    
        if st.button("Run Type Casting"):
            if(st.session_state.path != "" and len(files) > 0 and st.session_state.file != ""):
                with st.spinner("Computing complexity..."):
                    calculateComplexity(True, st.session_state.path, st.session_state.file, st.session_state.col)
                    st.success("Casting complete.")
                    st.subheader(f"First 10 rows of `{file_path[:-8]}_complexity.parquet`")
                    preview_df = executeCustomQueryDF(f"SELECT * FROM '{file_path[:-8]}_complexity.parquet' LIMIT 10")
                    st.dataframe(preview_df) 
            else:
                st.info("Please first select a directory with parquet files.")
        # test()


if __name__ == "__main__":
    main()