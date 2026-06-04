import csv
from pathlib import Path

import pandas as pd
import streamlit as st

from frontend.selector import selectFolder
from backend.similarity_over_time import (
    load_book_profiles,
    aggregate_mse,
    build_rows,
    weighted_average,
    field_pre_post_deltas,
    trend_checks,
    make_plots,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BOOKS = PROJECT_ROOT / "data" / "output" / "books_run" / "key_metrics_by_field.csv"
DEFAULT_MSE = PROJECT_ROOT / "data" / "output" / "mse_run" / "mse_windows.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "output" / "similarity_run"


def write_csv_safe(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def run_similarity_from_ui(
    books_path: Path,
    mse_path: Path,
    output_path: Path,
    min_words: int,
    create_plots: bool = True,
) -> dict:
    books_path = Path(books_path)
    mse_path = Path(mse_path)
    output_path = Path(output_path)

    if not books_path.exists():
        raise FileNotFoundError(f"Book metrics file not found: {books_path}")

    if not mse_path.exists():
        raise FileNotFoundError(f"MSE windows file not found: {mse_path}")

    output_path.mkdir(parents=True, exist_ok=True)

    book_profiles = load_book_profiles(books_path)
    groups = aggregate_mse(mse_path)
    rows = build_rows(book_profiles, groups, min_words)

    if not rows:
        raise ValueError(
            "No similarity rows were created. "
            "Try lowering min_words or check whether the field names match between book and MSE outputs."
        )

    monthly_overall = weighted_average(rows, ("month", "post_type"))
    pre_post = weighted_average(rows, ("period", "post_type"))
    field_deltas = field_pre_post_deltas(rows)
    trends = trend_checks(rows)

    output_files = {
        "book_similarity_over_time": output_path / "book_similarity_over_time.csv",
        "book_similarity_monthly_overall": output_path / "book_similarity_monthly_overall.csv",
        "book_similarity_pre_post": output_path / "book_similarity_pre_post.csv",
        "book_similarity_field_pre_post": output_path / "book_similarity_field_pre_post.csv",
        "book_similarity_trend_checks": output_path / "book_similarity_trend_checks.csv",
    }

    write_csv_safe(output_files["book_similarity_over_time"], rows)
    write_csv_safe(output_files["book_similarity_monthly_overall"], monthly_overall)
    write_csv_safe(output_files["book_similarity_pre_post"], pre_post)
    write_csv_safe(output_files["book_similarity_field_pre_post"], field_deltas)
    write_csv_safe(output_files["book_similarity_trend_checks"], trends)

    plot_files = {
        "Book similarity over time": output_path / "book_similarity_over_time.png",
        "Pre/Post similarity": output_path / "book_similarity_pre_post.png",
        "Delta by field": output_path / "book_similarity_delta_by_field.png",
    }

    if create_plots:
        make_plots(output_path, rows)

    return {
        "rows": rows,
        "monthly_overall": monthly_overall,
        "pre_post": pre_post,
        "field_deltas": field_deltas,
        "trends": trends,
        "output_files": output_files,
        "plot_files": plot_files,
        "output_path": output_path,
    }


def show_downloads(output_files: dict[str, Path]) -> None:
    st.subheader("Generated CSV files")

    for label, path in output_files.items():
        if path.exists():
            with path.open("rb") as f:
                st.download_button(
                    label=f"Download {path.name}",
                    data=f.read(),
                    file_name=path.name,
                    mime="text/csv",
                    key=f"download_{label}",
                )


def show_plots(plot_files: dict[str, Path]) -> None:
    st.subheader("Generated plots")

    for label, path in plot_files.items():
        if path.exists():
            st.image(str(path), caption=label)


def similaritySite():
    st.header("Book-Profile Similarity Over Time")

    st.write(
        "This analysis compares monthly Math StackExchange category profiles "
        "with textbook category profiles using cosine similarity."
    )

    if "similarity_output_path" not in st.session_state:
        st.session_state.similarity_output_path = str(DEFAULT_OUTPUT)

    books_path = st.text_input(
        "Book key metrics CSV",
        value=str(DEFAULT_BOOKS),
        help="Usually: data/output/books_run/key_metrics_by_field.csv",
    )

    mse_path = st.text_input(
        "MSE windows CSV",
        value=str(DEFAULT_MSE),
        help="Usually: data/output/mse_run/mse_windows.csv. This file is created by the MSE analyzer.",
    )

    if st.button("Select output folder"):
        selected = selectFolder()
        if selected:
            st.session_state.similarity_output_path = selected

    output_path = st.text_input(
        "Output folder",
        value=st.session_state.similarity_output_path,
        help="Similarity CSVs and plots will be written here.",
    )

    min_words = st.number_input(
        "Minimum words per month / field / post type",
        min_value=0,
        max_value=1_000_000,
        value=50_000,
        step=10_000,
    )

    create_plots = st.checkbox("Create plots", value=True)

    if st.button("Run similarity analysis"):
        try:
            with st.spinner("Computing book-profile similarity..."):
                result = run_similarity_from_ui(
                    books_path=Path(books_path),
                    mse_path=Path(mse_path),
                    output_path=Path(output_path),
                    min_words=int(min_words),
                    create_plots=create_plots,
                )

            st.success(
                f"Created {len(result['rows'])} monthly field/post-type similarity rows. "
                f"Output folder: {result['output_path']}"
            )

            st.subheader("Pre/Post summary")
            st.dataframe(pd.DataFrame(result["pre_post"]), use_container_width=True)

            st.subheader("Trend checks")
            st.dataframe(pd.DataFrame(result["trends"]), use_container_width=True)

            st.subheader("Field-level pre/post deltas")
            st.dataframe(pd.DataFrame(result["field_deltas"]), use_container_width=True)

            with st.expander("Show monthly similarity rows"):
                st.dataframe(pd.DataFrame(result["rows"]), use_container_width=True)

            show_plots(result["plot_files"])
            show_downloads(result["output_files"])

        except Exception as e:
            st.error(f"ERROR: {e}")