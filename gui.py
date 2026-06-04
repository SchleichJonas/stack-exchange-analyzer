import streamlit as st
from frontend.parserUI import parserSite
from frontend.describeUI import describeSite
from frontend.castingTablesUI import castingTablesSite
from frontend.complexityUI import complexitySite
from frontend.plotsUI import plotsSite
from frontend.customQueryUI import customQuerySite
from frontend.analyzeBooksUI import analyzeBooksSite
from frontend.mseAnalyzerUI import mseAnalyzerSite
from frontend.similarityUI import similaritySite


def main():
    """
    This launches the UI which is a local web server and opens it in a browser.
    """
    st.set_page_config(page_title="Stack exchange analyzer", layout="wide")

    st.sidebar.title("Actions")
    action = st.sidebar.radio(
        "Please select what you would like to do:",
        [
            "Parse XML to Parquet", 
            "Describe Tables", 
            "Cast Tables to Types", 
            "Calculate Complexity",
            "Plots",
            "Custom Query",
            "Analyze Books",
            "MSE Analyzer",
            "Books and MSE comparison",
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
        
    elif action == "Plots":
        plotsSite()
        
    elif action == "Custom Query":
        customQuerySite()

    elif action == "Analyze Books":
        analyzeBooksSite()

    elif action == "MSE Analyzer":
        mseAnalyzerSite()

    elif action == "Books and MSE comparison":
        similaritySite()


if __name__ == "__main__":
    main()