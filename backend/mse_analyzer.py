from __future__ import annotations

import argparse
import csv
import html
import re
import pandas as pd
from collections import defaultdict
from pathlib import Path
import pickle

from backend.analyzer import (
    DEFAULT_PATTERNS,
    ROOT,
    WORD_RE,
    analyze_window,
    key_metrics,
    load_patterns,
    make_plots,
    make_windows,
    normalize_text,
    summarize,
    write_csv,
)


DEFAULT_OUTPUT = ROOT / "output" / "mse_run"
TAG_RE = re.compile(r"<([^<>]+)>")
HTML_TAG_RE = re.compile(r"<[^>]+>")

DEFAULT_POSTS = ROOT / "data" / "Posts.parquet"
DEFAULT_OUTPUT = ROOT / "data" / "output" / "mse_run"

TAG_FIELD_MAP = {
    "calculus": "calculus",
    "integration": "calculus",
    "derivatives": "calculus",
    "sequences-and-series": "calculus",
    "multivariable-calculus": "calculus",
    "linear-algebra": "linear_algebra",
    "matrices": "linear_algebra",
    "vector-spaces": "linear_algebra",
    "eigenvalues-eigenvectors": "linear_algebra",
    "real-analysis": "real_analysis",
    "limits": "real_analysis",
    "continuity": "real_analysis",
    "metric-spaces": "real_analysis",
    "measure-theory": "advanced_analysis",
    "lebesgue-integral": "advanced_analysis",
    "functional-analysis": "advanced_analysis",
    "abstract-algebra": "abstract_algebra",
    "group-theory": "abstract_algebra",
    "ring-theory": "abstract_algebra",
    "field-theory": "abstract_algebra",
    "complex-analysis": "complex_analysis",
    "complex-numbers": "complex_analysis",
    "probability": "probability_statistics",
    "probability-theory": "probability_statistics",
    "statistics": "probability_statistics",
    "combinatorics": "discrete_math",
    "graph-theory": "discrete_math",
    "discrete-mathematics": "discrete_math",
    "number-theory": "number_theory",
    "elementary-number-theory": "number_theory",
    "modular-arithmetic": "number_theory",
    "geometry": "geometry",
    "euclidean-geometry": "geometry",
    "general-topology": "topology",
    "algebraic-topology": "topology",
    "ordinary-differential-equations": "differential_equations",
    "partial-differential-equations": "differential_equations",
    "numerical-methods": "numerical_analysis",
}


def clean_body(body: str) -> str:
    body = html.unescape(body or "")
    body = re.sub(r"<code>.*?</code>", " ", body, flags=re.DOTALL | re.IGNORECASE)
    body = HTML_TAG_RE.sub(" ", body)
    return normalize_text(body)


def parse_tags(tag_text: str) -> list[str]:
    tag_text = tag_text or ""
    if "|" in tag_text:
        return [tag for tag in tag_text.strip("|").split("|") if tag]
    return TAG_RE.findall(tag_text)


def map_field(tags: list[str]) -> str:
    counts = defaultdict(int)
    for tag in tags:
        field = TAG_FIELD_MAP.get(tag)
        if field:
            counts[field] += 1
    if not counts:
        return "other"
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def clean_parquet_value(value) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def iter_posts(path: Path):
    columns = [
        "Id",
        "PostTypeId",
        "ParentId",
        "Body",
        "Tags",
        "CreationDate",
        "Score",
    ]

    if path.is_dir():
        files = sorted(path.glob("*.parquet"))
        df = pd.concat(
            [pd.read_parquet(file, columns=columns) for file in files],
            ignore_index=True,
        )
    else:
        df = pd.read_parquet(path, columns=columns)

    df = df.sort_values("Id")

    for row in df.to_dict("records"):
        yield {key: clean_parquet_value(value) for key, value in row.items()}


def post_type_name(post_type_id: str) -> str:
    if post_type_id == "1":
        return "question"
    if post_type_id == "2":
        return "answer"
    return "other"


def percentile(values: list[int], pct: float) -> float:
    if not values:
        return 0
    values = sorted(values)
    idx = round((len(values) - 1) * pct)
    return values[idx]


def post_length_summary(lengths: dict[str, list[int]]) -> list[dict]:
    rows = []
    for post_type, values in sorted(lengths.items()):
        if not values:
            continue
        total = sum(values)
        rows.append(
            {
                "post_type": post_type,
                "post_count": len(values),
                "mean_words": round(total / len(values), 2),
                "median_words": percentile(values, 0.5),
                "p75_words": percentile(values, 0.75),
                "p90_words": percentile(values, 0.9),
                "p95_words": percentile(values, 0.95),
                "max_words": max(values),
                "share_under_50_words": round(sum(v < 50 for v in values) / len(values), 4),
                "share_under_200_words": round(sum(v < 200 for v in values) / len(values), 4),
            }
        )
    return rows


def analyze_posts(
    posts_path: Path,
    output: Path,
    window: int,
    stride: int,
    limit: int | None,
    min_words: int,
):
    patterns = load_patterns(DEFAULT_PATTERNS)
    output.mkdir(parents=True, exist_ok=True)
    window_rows = []
    question_tags = {}
    question_fields = {}
    post_lengths = defaultdict(list)
    seen = 0
    kept = 0
    for post in iter_posts(posts_path):
        seen += 1
        if limit and seen > limit:
            break
        post_type = post_type_name(post.get("PostTypeId", ""))
        if post_type == "other":
            continue

        body = clean_body(post.get("Body", ""))
        if not body:
            continue

        word_count = len(WORD_RE.findall(body))
        if word_count < min_words:
            continue
        post_lengths[post_type].append(word_count)

        post_id = post.get("Id", "")
        parent_id = post.get("ParentId", "")
        if post_type == "question":
            tags = parse_tags(post.get("Tags", ""))
            field = map_field(tags)
            question_tags[post_id] = tags
            question_fields[post_id] = field
        else:
            tags = question_tags.get(parent_id, [])
            field = question_fields.get(parent_id, "other")
        for window_id, chunk in make_windows(body, window, stride):
            row = analyze_window(chunk, patterns)
            row.update(
                {
                    "source_type": "math_stackexchange",
                    "field": field,
                    "post_id": post_id,
                    "parent_id": parent_id,
                    "post_type": post_type,
                    "creation_date": post.get("CreationDate", ""),
                    "score": post.get("Score", ""),
                    "tags": " ".join(tags),
                    "window_id": window_id,
                }
            )
            window_rows.append(row)
        kept += 1
        if kept % 50000 == 0:
            print(f"Processed {kept} question/answer posts...")

    field_summary = summarize(window_rows, "field")
    post_type_summary = summarize(window_rows, "post_type")
    write_csv(output / "mse_windows.csv", window_rows)
    write_csv(output / "mse_summary_by_field.csv", field_summary)
    write_csv(output / "mse_summary_by_post_type.csv", post_type_summary)
    write_csv(output / "mse_post_length_summary.csv", post_length_summary(post_lengths))
    write_csv(output / "mse_key_metrics_by_field.csv", key_metrics(field_summary))
    data_dir = posts_path.resolve().parent / "plot_inputs.pkl"
    with open(data_dir, "wb") as f:
        pickle.dump((output, field_summary, window_rows), f)


def main(DEFAULT_POSTS):
    parser = argparse.ArgumentParser(description="Analyze Math StackExchange parquet with book-analyzer metrics.")
    parser.add_argument("--posts", type=Path, default=DEFAULT_POSTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--window", type=int, default=200)
    parser.add_argument("--stride", type=int, default=200)
    parser.add_argument("--limit", type=int, default=None, help="Optional XML row limit for quick tests.")
    parser.add_argument("--min-words", type=int, default=0, help="Skip posts shorter than this word count.")
    args = parser.parse_args()
    analyze_posts(
        args.posts,
        args.output,
        args.window,
        args.stride,
        args.limit,
        args.min_words,
    )

