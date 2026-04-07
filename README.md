# Mathoverflow and Math Steckexchange usage analysis in relation to AI

**Work In Progress!** This project is far from finished. It may contain bugs, might crash, and the codebase will likely receive complete overhauls. This is a working repository for a university seminar paper. Please keep this in mind! Thank you.

## General
With our project we want to analyse the usage and overall change that might or might not have happened due to AI in the stack exchange network.
Our goal is to determine if the release of popular generative AI like ChatGPT, Gemini or similar had a significant impact on those platforms.
Further we want to find out if a certain release of an AI like ChatGPT2 for example had a bigger impact than the initial release,
because there might have been substantial improvements to its capabilities. In our research we also want to determine the difficulty
of the questions and whether it has changed or not. Determining difficulty of mathematical questions on a large scale will not be 100% accurate,
but we will imploy multiple techniques like question length, number of fomulars, length of formulas, length of answers and their formulas and possibly more.

## Usage
This section will cover how to use the tools we built for our analysis

### Prerequisites
- Python3 (Tested on `Python3.13` and `Python 3.14`)
- Stackexchange data dump (Only mathoverflow.net.7z and math.stackexchange.com.7z was used from 2025-09-30 from the <a href="https://archive.org/details/stackexchange_20250930" title="stackexchange_20250930">Internet Archive</a>, but in theory other dumps/files should work just fine)
- Put the XML files in a subdirectory of the projects base directory so the program finds the files.
- tkinter should work on Windows and MacOS out of the box, but on Linux you need to install it using the system's package manager

### Installation
To install all necessary requirements use:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To install tkinter on Linx:
**Fedora**
```bash
sudo dnf install python3-tkinter
```
**Arch**
```bash
sudo pacman -S tk
```
**Ubuntu**
```bash
sudo apt update
sudo apt install python3-tk
```

### Usage
We provide a GUI in form of a local webserver. When executing `gui.py` a browser window opens with the options equivalent to the command line.
It is recommendet to use the GUI, but we will try to provide all functions also to the command line.
```bash
streamlit run gui.py
```


The `main.py` file start the command line interface containing a menu from which all necessary functions can be called.
```bash
python main.py
```
When executing `main.py` you will be presented with the following menu:

Please select on what you would like to do:
1 Parse XML files to parquet files
2 Describe all tables
3 Cast tables to correct types (creates new files called [tableName]_typed.parquet)
4 Exit

- 0: Exits the program
- 1: To parse the XML files to parquet files the first option will parse all XML files of a given directory
- 2: You will get and SQL DESCRIBE querry for all tables. If the tables are already casted then those will be taken, otherwise the raw parquet files are taken.
- 3: This option will convert all columns to the correct types. When creating the parquet files all columns will be of type `VARCHAR`. Therefore we need to convert them to the approapriate types to compute statistics on them.
- 4: Calculate complexity of a file
