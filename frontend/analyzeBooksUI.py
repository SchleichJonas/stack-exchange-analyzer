import csv
from pathlib import Path

import pandas as pd
import streamlit as st

from frontend.selector import selectFolder
from backend.analyzer import (
    DEFAULT_CORPUS,
    DEFAULT_PATTERNS,
    DEFAULT_OUTPUT,
    load_patterns,
    discover_books,
    read_text,
    make_windows,
    analyze_window,
    summarize,
    key_metrics,
    write_csv,
    make_plots,
    relative_label,
)


DEFAULT_BOOK_OUTPUT = DEFAULT_OUTPUT / "books_run"


def read_csv_rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def expected_output_files(output_path: Path) -> dict[str, Path]:
    return {
        "book_windows": output_path / "book_windows.csv",
        "book_summary_by_file": output_path / "book_summary_by_file.csv",
        "book_summary_by_field": output_path / "book_summary_by_field.csv",
        "key_metrics_by_field": output_path / "key_metrics_by_field.csv",
    }


def load_existing_book_analysis(output_path: Path) -> dict:
    files = expected_output_files(output_path)

    missing = [str(path) for path in files.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing output files:\n" + "\n".join(missing)
        )

    window_rows = read_csv_rows(files["book_windows"])
    file_summary = read_csv_rows(files["book_summary_by_file"])
    field_summary = read_csv_rows(files["book_summary_by_field"])
    key_metrics_rows = read_csv_rows(files["key_metrics_by_field"])

    figures = make_plots(field_summary, window_rows)

    return {
        "mode": "loaded existing output",
        "window_rows": window_rows,
        "file_summary": file_summary,
        "field_summary": field_summary,
        "key_metrics": key_metrics_rows,
        "figures": figures,
        "output_files": files,
        "output_path": output_path,
    }


def run_book_analysis(
    corpus_path: Path,
    patterns_path: Path,
    output_path: Path,
    window: int,
    stride: int,
) -> dict:
    corpus_path = Path(corpus_path)
    patterns_path = Path(patterns_path)
    output_path = Path(output_path)

    if not corpus_path.exists():
        raise FileNotFoundError(f"Corpus folder not found: {corpus_path}")

    if not patterns_path.exists():
        raise FileNotFoundError(f"Pattern file not found: {patterns_path}")

    output_path.mkdir(parents=True, exist_ok=True)

    patterns = load_patterns(patterns_path)
    books = discover_books(corpus_path)

    if not books:
        raise ValueError(
            f"No .txt or .pdf files found in {corpus_path}. "
            "Expected structure: corpus/books/<field>/<book>.pdf or .txt"
        )

    window_rows = []

    progress = st.progress(0)
    status = st.empty()

    for i, book in enumerate(books):
        path = book["path"]

        title = book.get("title") or path.stem.replace("_", " ")
        field = book.get("field") or path.parent.name
        document_id = book.get("document_id") or path.stem

        status.write(f"Reading `{title}`...")
        text = read_text(path)

        for window_id, chunk in make_windows(text, window, stride):
            row = analyze_window(chunk, patterns)
            row.update(
                {
                    "document_id": document_id,
                    "title": title,
                    "field": field,
                    "filepath": relative_label(path),
                    "window_id": window_id,
                }
            )
            window_rows.append(row)

        progress.progress((i + 1) / len(books))

    status.empty()
    progress.empty()

    file_summary = summarize(window_rows, "document_id")
    field_summary = summarize(window_rows, "field")
    key_metrics_rows = key_metrics(field_summary)

    files = expected_output_files(output_path)
    write_csv(files["book_windows"], window_rows)
    write_csv(files["book_summary_by_file"], file_summary)
    write_csv(files["book_summary_by_field"], field_summary)
    write_csv(files["key_metrics_by_field"], key_metrics_rows)

    figures = make_plots(field_summary, window_rows)

    return {
        "mode": "recalculated",
        "window_rows": window_rows,
        "file_summary": file_summary,
        "field_summary": field_summary,
        "key_metrics": key_metrics_rows,
        "figures": figures,
        "output_files": files,
        "output_path": output_path,
    }


def show_result(result: dict) -> None:
    st.success(
        f"Book analysis {result['mode']}. "
        f"Output folder: `{result['output_path']}`"
    )

    st.subheader("Plots")

    figures = result.get("figures", {})
    if figures:
        for name, fig in figures.items():
            st.write(f"### {name.replace('_', ' ').title()}")
            st.pyplot(fig)
    else:
        st.warning("No plots available.")

    st.subheader("Key metrics by field")
    st.dataframe(pd.DataFrame(result["key_metrics"]), use_container_width=True)

    with st.expander("Book summary by field"):
        st.dataframe(pd.DataFrame(result["field_summary"]), use_container_width=True)

    with st.expander("Book summary by file"):
        st.dataframe(pd.DataFrame(result["file_summary"]), use_container_width=True)

    with st.expander("Raw window rows"):
        st.dataframe(pd.DataFrame(result["window_rows"]), use_container_width=True)

    st.subheader("Download CSV output")

    for name, path in result["output_files"].items():
        if path.exists():
            with path.open("rb") as f:
                st.download_button(
                    label=f"Download {path.name}",
                    data=f.read(),
                    file_name=path.name,
                    mime="text/csv",
                    key=f"download_books_{name}",
                )


def analyzeBooksSite():
    st.header("Analyze Book Corpus")

    st.write(
        "Analyze university-level mathematics books, write CSV summaries, "
        "and display the generated plots directly in the GUI."
    )

    if "book_corpus_path" not in st.session_state:
        st.session_state.book_corpus_path = str(DEFAULT_CORPUS)

    if "book_output_path" not in st.session_state:
        st.session_state.book_output_path = str(DEFAULT_BOOK_OUTPUT)

    mode = st.radio(
        "Analysis mode",
        ["Use existing output", "Recalculate"],
        horizontal=True,
    )

    if st.button("Select output folder"):
        selected = selectFolder()
        if selected:
            st.session_state.book_output_path = selected

    output_path = st.text_input(
        "Output folder",
        value=st.session_state.book_output_path,
    )

    output_path = Path(output_path)

    if mode == "Use existing output":
        st.info(
            "This uses existing CSV files from the selected output folder. "
            "No books are reprocessed."
        )

        if st.button("Load existing book analysis"):
            try:
                with st.spinner("Loading existing book analysis..."):
                    result = load_existing_book_analysis(output_path)
                    st.session_state.book_analysis_result = result

            except Exception as e:
                st.error(f"ERROR: {e}")

    else:
        st.info(
            "This recalculates the book analysis from the corpus folder "
            "and overwrites the CSV files in the output folder."
        )

        if st.button("Select corpus folder"):
            selected = selectFolder()
            if selected:
                st.session_state.book_corpus_path = selected

        corpus_path = st.text_input(
            "Book corpus folder",
            value=st.session_state.book_corpus_path,
            help="Expected structure: data/corpus/books/<field>/<book>.pdf or .txt",
        )

        patterns_path = st.text_input(
            "Math patterns CSV",
            value=str(DEFAULT_PATTERNS),
        )

        window = st.number_input(
            "Window size",
            min_value=50,
            max_value=1000,
            value=200,
            step=50,
        )

        stride = st.number_input(
            "Stride",
            min_value=50,
            max_value=1000,
            value=200,
            step=50,
        )

        if st.button("Run book analysis"):
            try:
                with st.spinner("Analyzing books..."):
                    result = run_book_analysis(
                        corpus_path=Path(corpus_path),
                        patterns_path=Path(patterns_path),
                        output_path=output_path,
                        window=int(window),
                        stride=int(stride),
                    )
                    st.session_state.book_analysis_result = result

            except Exception as e:
                st.error(f"ERROR: {e}")

    if "book_analysis_result" in st.session_state:
        show_result(st.session_state.book_analysis_result)