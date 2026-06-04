from __future__ import annotations

import argparse
import csv
import html
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from pypdf import PdfReader
import pickle


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "data" / "corpus" / "books"
DEFAULT_PATTERNS = ROOT / "data" / "math_patterns.csv"
DEFAULT_OUTPUT = ROOT / "data" / "output"

WORD_RE = re.compile(r"[A-Za-z]{2,}(?:[-'][A-Za-z]+)?|\d+(?:\.\d+)?")
EXPLICIT_FORMULA_RE = re.compile(r"\$\$.*?\$\$|\$.*?\$|\\\[.*?\\\]|\\\(.*?\\\)", re.DOTALL)
FUNCTION_CALL_RE = re.compile(
    r"""
    (?<![A-Za-z0-9_\\])
    (?:[fghFTS]|[A-Za-z]\\?[A-Za-z]{1,12})
    (?:_\{?[A-Za-z0-9]+\}?)?
    \s*\(\s*
    [A-Za-z0-9\\{}_^+\-*/,\s]{1,40}
    \s*\)
    """,
    re.VERBOSE,
)
MATH_FRAGMENT_RE = re.compile(
    r"""
    (?:[A-Za-zΑ-Ωα-ω][\w{}\\]*(?:\s*[_^]\s*[\w{}\\]+)?\s*
       (?:=|≤|≥|≠|≈|<|>|∈|∉|⊂|⊆|→|⇒|\\leq|\\geq|\\neq|\\approx|\\in|\\to|\\Rightarrow)
       \s*[^,.;:\n]{1,80})
    |
    (?:(?:\\frac|\\sqrt|\\sum|\\prod|\\int|\\lim|\\begin\{(?:matrix|pmatrix|bmatrix|array|align|equation)\})
       [^\n]{0,120})
    """,
    re.VERBOSE,
)


def normalize_text(text: str) -> str:
    text = html.unescape(text)
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def read_text(path: Path) -> str:
    if path.suffix.lower() == ".pdf":
        reader = PdfReader(str(path))
        pages = []
        skipped = 0
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                skipped += 1
                pages.append("")
        if skipped:
            print(f"Warning: skipped text extraction on {skipped} page(s) in {path.name}")
        return normalize_text("\n\n".join(pages))

    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return normalize_text(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            pass
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Cannot decode {path}")


def load_patterns(path: Path) -> list[dict]:
    patterns = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row.get("pattern"):
                continue
            flags = 0 if row.get("case_sensitive", "yes").lower() == "yes" else re.IGNORECASE
            match_type = row.get("match_type", "literal").strip()
            pattern = row["pattern"].strip()
            if match_type == "literal":
                regex = re.compile(re.escape(pattern), flags)
            elif match_type == "word":
                regex = re.compile(rf"(?<![A-Za-z0-9_\\]){re.escape(pattern)}(?![A-Za-z0-9_])", flags)
            elif match_type == "regex":
                regex = re.compile(pattern, flags)
            else:
                raise ValueError(f"Unknown match_type {match_type!r} in {path}")
            row["regex"] = regex
            patterns.append(row)
    return patterns



def discover_books(corpus: Path) -> list[dict]:
    books = []
    files = sorted([*corpus.rglob("*.txt"), *corpus.rglob("*.pdf")])
    for path in files:
        books.append(
            {
                "path": path,
                "document_id": path.stem,
            }
        )
    return books


def relative_label(path: Path) -> str:
    path = path.resolve()
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def make_windows(text: str, size: int, stride: int) -> list[tuple[int, str]]:
    words = list(WORD_RE.finditer(text))
    if not words:
        return [(0, text)] if text else []
    windows = []
    for start_i in range(0, len(words), stride):
        next_i = min(start_i + size, len(words))
        chunk_start = 0 if start_i == 0 else words[start_i].start()
        chunk_end = len(text) if next_i == len(words) else words[next_i].start()
        chunk = text[chunk_start:chunk_end]
        windows.append((len(windows), chunk))
        if next_i == len(words):
            break
    return windows


def count_patterns(text: str, patterns: list[dict]) -> Counter:
    by_category = Counter()
    by_label = Counter()
    for row in patterns:
        count = len(row["regex"].findall(text))
        if count:
            by_category[row["category"]] += count
            by_label[row["label"]] += count
    return by_category


def formula_stats(text: str) -> dict:
    explicit = [m.group(0) for m in EXPLICIT_FORMULA_RE.finditer(text)]
    display = [x for x in explicit if x.startswith("$$") or x.startswith("\\[")]
    text_without_explicit = EXPLICIT_FORMULA_RE.sub(" ", text)
    implicit = [m.group(0) for m in MATH_FRAGMENT_RE.finditer(text_without_explicit)]
    formulas = explicit + implicit
    lengths = [len(x) for x in formulas]
    return {
        "formula_count": len(formulas),
        "explicit_formula_count": len(explicit),
        "implicit_formula_count": len(implicit),
        "inline_formula_count": len(explicit) - len(display),
        "display_formula_count": len(display),
        "formula_char_count": sum(lengths),
        "avg_formula_length": round(sum(lengths) / len(lengths), 2) if lengths else 0,
        "max_formula_length": max(lengths) if lengths else 0,
    }


def analyze_window(text: str, patterns: list[dict]) -> dict:
    word_count = len(WORD_RE.findall(text))
    char_count = len(text)
    categories = count_patterns(text, patterns)
    formulas = formula_stats(text)
    function_call_count = len(FUNCTION_CALL_RE.findall(text))
    math_token_count = sum(v for k, v in categories.items() if not k.startswith("marker_"))
    row = {
        "word_count": word_count,
        "char_count": char_count,
        **formulas,
        "function_call_count": function_call_count,
        "math_token_count": math_token_count,
        "formula_density_per_200_words": round(formulas["formula_count"] * 200 / max(word_count, 1), 4),
        "function_call_density_per_200_words": round(function_call_count * 200 / max(word_count, 1), 4),
        "math_token_density_per_200_words": round(math_token_count * 200 / max(word_count, 1), 4),
        "formula_char_share": round(formulas["formula_char_count"] / max(char_count, 1), 4),
    }
    for category, count in categories.items():
        row[f"category_{category}"] = count
    return row


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def number(row: dict, key: str) -> float:
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def summarize(rows: list[dict], group_key: str) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[row[group_key]].append(row)
    category_keys = sorted({key for row in rows for key in row if key.startswith("category_")})
    summary = []
    for group, items in sorted(groups.items()):
        total_words = sum(int(x["word_count"]) for x in items)
        total_chars = sum(int(x["char_count"]) for x in items)
        total_formulas = sum(int(x["formula_count"]) for x in items)
        total_formula_chars = sum(int(x["formula_char_count"]) for x in items)
        total_explicit = sum(int(x["explicit_formula_count"]) for x in items)
        total_implicit = sum(int(x["implicit_formula_count"]) for x in items)
        total_inline = sum(int(x["inline_formula_count"]) for x in items)
        total_display = sum(int(x["display_formula_count"]) for x in items)
        total_function_calls = sum(int(x["function_call_count"]) for x in items)
        total_math = sum(int(x["math_token_count"]) for x in items)
        summary.append(
            {
                group_key: group,
                "window_count": len(items),
                "word_count": total_words,
                "char_count": total_chars,
                "formula_count": total_formulas,
                "explicit_formula_count": total_explicit,
                "implicit_formula_count": total_implicit,
                "inline_formula_count": total_inline,
                "display_formula_count": total_display,
                "formula_char_count": total_formula_chars,
                "function_call_count": total_function_calls,
                "math_token_count": total_math,
                "avg_formula_length": round(total_formula_chars / max(total_formulas, 1), 4),
                "formula_char_share": round(total_formula_chars / max(total_chars, 1), 4),
                "formula_density_per_200_words": round(total_formulas * 200 / max(total_words, 1), 4),
                "explicit_formula_density_per_200_words": round(total_explicit * 200 / max(total_words, 1), 4),
                "implicit_formula_density_per_200_words": round(total_implicit * 200 / max(total_words, 1), 4),
                "function_call_density_per_200_words": round(total_function_calls * 200 / max(total_words, 1), 4),
                "math_token_density_per_200_words": round(total_math * 200 / max(total_words, 1), 4),
            }
        )
        for meta_key in ("title", "field", "level", "source", "license", "filepath"):
            values = sorted({x.get(meta_key, "") for x in items if x.get(meta_key, "")})
            if len(values) == 1:
                summary[-1][meta_key] = values[0]
        for key in category_keys:
            total = sum(int(x.get(key) or 0) for x in items)
            summary[-1][key] = total
            summary[-1][f"{key}_per_200_words"] = round(total * 200 / max(total_words, 1), 4)
    return summary


def key_metrics(field_summary: list[dict]) -> list[dict]:
    keys = [
        "field",
        "window_count",
        "word_count",
        "formula_density_per_200_words",
        "math_token_density_per_200_words",
        "function_call_density_per_200_words",
        "explicit_formula_density_per_200_words",
        "implicit_formula_density_per_200_words",
        "avg_formula_length",
        "formula_char_share",
        "category_greek_per_200_words",
        "category_relation_per_200_words",
        "category_set_theory_per_200_words",
        "category_logic_per_200_words",
        "category_calculus_per_200_words",
        "category_linear_algebra_per_200_words",
        "category_probability_statistics_per_200_words",
        "category_number_theory_per_200_words",
        "category_abstract_algebra_per_200_words",
        "category_geometry_per_200_words",
        "category_topology_per_200_words",
        "category_combinatorics_per_200_words",
    ]
    return [{key: row.get(key, 0) for key in keys} for row in field_summary]


def values_by_field(rows: list[dict], metric: str) -> tuple[list[str], list[list[float]]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["field"]].append(number(row, metric))
    names = sorted(grouped)
    return names, [grouped[name] for name in names]


def make_plots(path):
    data_dir = path + "/plot_inputs.pkl"
    with open(data_dir, "rb") as f:
        output, field_summary, window_rows = pickle.load(f)
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return {}

    figures = {}

    if not field_summary:
        return figures

    names = [r["field"] for r in field_summary]
    formulas = [r["formula_density_per_200_words"] for r in field_summary]
    symbols = [r["math_token_density_per_200_words"] for r in field_summary]
    fig, ax = plt.subplots(figsize=(max(8, len(names) * 0.8), 5))
    x = range(len(names))
    ax.bar(x, formulas, width=0.4, label="formulas / 200 words")
    ax.bar([i + 0.4 for i in x], symbols, width=0.4, label="math tokens / 200 words")
    ax.set_xticks([i + 0.2 for i in x])
    ax.set_xticklabels(names, rotation=45, ha="right")
    ax.set_ylabel("density")
    ax.legend()
    fig.tight_layout()
    figures["field_density"] = fig

    function_calls = [r["function_call_density_per_200_words"]for r in field_summary]
    fig, ax = plt.subplots(figsize=(max(8, len(names) * 0.8), 5))
    ax.bar(names, function_calls)
    ax.set_ylabel("function calls / 200 words")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    figures["function_call_density"] = fig

    box_names, box_values = values_by_field(window_rows,"formula_density_per_200_words",)
    fig, ax = plt.subplots(figsize=(max(8, len(box_names) * 0.8), 5))
    ax.boxplot(box_values,labels=box_names,showfliers=False,)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_ylabel("formulas / 200 words per window")
    fig.tight_layout()
    figures["formula_density_distribution"] = fig
    explicit = [r["explicit_formula_density_per_200_words"] for r in field_summary]
    implicit = [r["implicit_formula_density_per_200_words"]for r in field_summary]
    fig, ax = plt.subplots(figsize=(max(8, len(names) * 0.8), 5))
    ax.bar(names,explicit,label="explicit LaTeX/MathJax",)
    ax.bar(names,implicit,bottom=explicit,label="implicit fragments",)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_ylabel("formulas / 200 words")
    ax.legend()
    fig.tight_layout()
    figures["formula_source_density"] = fig

    categories = [
        "calculus",
        "linear_algebra",
        "set_theory",
        "logic",
        "greek",
        "probability_statistics",
        "number_theory",
        "abstract_algebra",
        "geometry",
        "topology",
        "combinatorics",
    ]

    data = [
        [
            number(row, f"category_{cat}_per_200_words")
            for cat in categories
        ]
        for row in field_summary
    ]

    fig, ax = plt.subplots(figsize=(max(9, len(categories) * 0.75),max(5, len(names) * 0.35),))
    im = ax.imshow(data, aspect="auto")
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=45, ha="right")
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names)
    fig.colorbar(im, ax=ax, label="tokens / 200 words")
    fig.tight_layout()
    figures["category_density_heatmap"] = fig

    return figures


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze mathematical density in plain-text books.")
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--patterns", type=Path, default=DEFAULT_PATTERNS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--window", type=int, default=200)
    parser.add_argument("--stride", type=int, default=200)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    patterns = load_patterns(args.patterns)
    books = discover_books(args.corpus)
    if not books:
        print(f"No .txt files found in {args.corpus}. Put books into corpus/books/<field>/ and run again.")
        return

    window_rows = []
    
    for book in books:
        print(f"Reading {book['title']}...")
        text = read_text(book["path"])
        for window_id, chunk in make_windows(text, args.window, args.stride):
            row = analyze_window(chunk, patterns)
            row.update({k: v for k, v in book.items() if k != "path"})
            row["filepath"] = relative_label(book["path"])
            row["window_id"] = window_id
            window_rows.append(row)

    file_summary = summarize(window_rows, "document_id")
    field_summary = summarize(window_rows, "field")
    write_csv(args.output / "book_windows.csv", window_rows)
    write_csv(args.output / "book_summary_by_file.csv", file_summary)
    write_csv(args.output / "book_summary_by_field.csv", field_summary)
    write_csv(args.output / "key_metrics_by_field.csv", key_metrics(field_summary))
    make_plots(args.output, field_summary, window_rows)
    print(f"Analyzed {len(books)} books and {len(window_rows)} windows.")
    print(f"Wrote CSV output to {args.output}")


if __name__ == "__main__":
    main()
