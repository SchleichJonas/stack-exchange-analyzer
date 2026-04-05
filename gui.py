import streamlit as st
from frontend.parserUI import parserSite
from frontend.describeUI import describeSite
from frontend.castingTablesUI import castingTablesSite
from frontend.complexityUI import complexitySite


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
        parserSite()

    elif action == "Describe Tables":
        describeSite()

    elif action == "Cast Tables to Types":
        castingTablesSite()
            
    elif action == "Calculate Complexity":
        complexitySite()


if __name__ == "__main__":
    main()