from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BOOKS = ROOT / "data" / "output" / "books_run" / "key_metrics_by_field.csv"
DEFAULT_MSE = ROOT / "data" / "output" / "mse_run" / "mse_windows.csv"
DEFAULT_OUTPUT = ROOT / "data" / "output" / "similarity_run"
CHATGPT_MONTH = "2022-12"
PLACEBO_MONTH = "2021-12"

CATEGORIES = [
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


def num(row: dict, key: str) -> float:
    try:
        return float(row.get(key) or 0)
    except ValueError:
        return 0.0


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def load_book_profiles(path: Path) -> dict[str, list[float]]:
    with path.open(newline="", encoding="utf-8") as f:
        profiles = {}
        for row in csv.DictReader(f):
            profiles[row["field"]] = [num(row, f"category_{cat}_per_200_words") for cat in CATEGORIES]
        return profiles


def aggregate_mse(path: Path) -> dict[tuple[str, str, str], dict]:
    groups = defaultdict(lambda: {"word_count": 0.0, "window_count": 0, **{cat: 0.0 for cat in CATEGORIES}})
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            field = row["field"]
            if field == "other":
                continue
            month = row["creation_date"][:7]
            key = (month, field, row["post_type"])
            groups[key]["word_count"] += num(row, "word_count")
            groups[key]["window_count"] += 1
            for cat in CATEGORIES:
                groups[key][cat] += num(row, f"category_{cat}")
    return groups


def build_rows(book_profiles: dict[str, list[float]], groups: dict, min_words: int) -> list[dict]:
    rows = []
    for (month, field, post_type), data in sorted(groups.items()):
        if field not in book_profiles or data["word_count"] < min_words:
            continue
        mse_profile = [data[cat] * 200 / data["word_count"] for cat in CATEGORIES]
        rows.append(
            {
                "month": month,
                "field": field,
                "post_type": post_type,
                "period": "post_chatgpt" if month >= CHATGPT_MONTH else "pre_chatgpt",
                "word_count": round(data["word_count"]),
                "window_count": data["window_count"],
                "book_similarity": round(cosine(book_profiles[field], mse_profile), 5),
            }
        )
    return rows


def run_similarity(
    books_path: Path | str = DEFAULT_BOOKS,
    mse_path: Path | str = DEFAULT_MSE,
    output: Path | str = DEFAULT_OUTPUT,
    min_words: int = 50000,
    create_plots: bool = True,
) -> dict:
    books_path = Path(books_path)
    mse_path = Path(mse_path)
    output = Path(output)

    output.mkdir(parents=True, exist_ok=True)

    if not books_path.exists():
        raise FileNotFoundError(f"Book metrics file not found: {books_path}")

    if not mse_path.exists():
        raise FileNotFoundError(f"MSE windows file not found: {mse_path}")

    book_profiles = load_book_profiles(books_path)
    groups = aggregate_mse(mse_path)
    rows = build_rows(book_profiles, groups, min_words)

    if not rows:
        raise ValueError(
            "No similarity rows were created. "
            "Try lowering min_words or check whether field names match between books and MSE."
        )

    monthly_overall = weighted_average(rows, ("month", "post_type"))
    pre_post = weighted_average(rows, ("period", "post_type"))
    field_deltas = field_pre_post_deltas(rows)
    trends = trend_checks(rows)

    output_files = {
        "book_similarity_over_time": output / "book_similarity_over_time.csv",
        "book_similarity_monthly_overall": output / "book_similarity_monthly_overall.csv",
        "book_similarity_pre_post": output / "book_similarity_pre_post.csv",
        "book_similarity_field_pre_post": output / "book_similarity_field_pre_post.csv",
        "book_similarity_trend_checks": output / "book_similarity_trend_checks.csv",
    }

    write_csv(output_files["book_similarity_over_time"], rows)
    write_csv(output_files["book_similarity_monthly_overall"], monthly_overall)
    write_csv(output_files["book_similarity_pre_post"], pre_post)
    write_csv(output_files["book_similarity_field_pre_post"], field_deltas)
    write_csv(output_files["book_similarity_trend_checks"], trends)

    plot_files = {}
    if create_plots:
        make_plots(output, rows)
        plot_files = {
            "over_time": output / "book_similarity_over_time.png",
            "pre_post": output / "book_similarity_pre_post.png",
            "delta_by_field": output / "book_similarity_delta_by_field.png",
        }

    return {
        "rows": rows,
        "monthly_overall": monthly_overall,
        "pre_post": pre_post,
        "field_deltas": field_deltas,
        "trends": trends,
        "output_files": output_files,
        "plot_files": plot_files,
        "output_dir": output,
    }

def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def weighted_average(rows: list[dict], group_keys: tuple[str, ...]) -> list[dict]:
    groups = defaultdict(lambda: {"weighted": 0.0, "words": 0.0})
    for row in rows:
        key = tuple(row[k] for k in group_keys)
        words = num(row, "word_count")
        groups[key]["weighted"] += num(row, "book_similarity") * words
        groups[key]["words"] += words
    out = []
    for key, data in sorted(groups.items()):
        row = {name: value for name, value in zip(group_keys, key)}
        row["book_similarity"] = round(data["weighted"] / data["words"], 5) if data["words"] else 0
        row["word_count"] = round(data["words"])
        out.append(row)
    return out


def month_index(month: str) -> int:
    year, mon = month.split("-")
    return int(year) * 12 + int(mon)


def slope(points: list[tuple[int, float]]) -> float:
    if len(points) < 2:
        return 0.0
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    xbar = sum(xs) / len(xs)
    ybar = sum(ys) / len(ys)
    denom = sum((x - xbar) ** 2 for x in xs)
    return sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys)) / denom if denom else 0.0


def trend_checks(rows: list[dict], cutoffs: tuple[str, ...] = (CHATGPT_MONTH, PLACEBO_MONTH)) -> list[dict]:
    monthly = weighted_average(rows, ("month", "post_type"))
    out = []
    for cutoff in cutoffs:
        for post_type in ("question", "answer"):
            values = [r for r in monthly if r["post_type"] == post_type and r["month"] >= "2013-01"]
            pre = [r for r in values if r["month"] < cutoff]
            post = [r for r in values if r["month"] >= cutoff]
            if not pre or not post:
                continue
            pre_mean = sum(num(r, "book_similarity") for r in pre) / len(pre)
            post_mean = sum(num(r, "book_similarity") for r in post) / len(post)
            pre_slope = slope([(month_index(r["month"]), num(r, "book_similarity")) for r in pre])
            post_slope = slope([(month_index(r["month"]), num(r, "book_similarity")) for r in post])
            out.append(
                {
                    "cutoff": cutoff,
                    "kind": "chatgpt" if cutoff == CHATGPT_MONTH else "placebo",
                    "post_type": post_type,
                    "pre_months": len(pre),
                    "post_months": len(post),
                    "pre_mean": round(pre_mean, 5),
                    "post_mean": round(post_mean, 5),
                    "mean_diff_post_minus_pre": round(post_mean - pre_mean, 5),
                    "pre_slope_per_month": round(pre_slope, 7),
                    "post_slope_per_month": round(post_slope, 7),
                    "slope_diff_post_minus_pre": round(post_slope - pre_slope, 7),
                }
            )
    return out


def field_pre_post_deltas(rows: list[dict]) -> list[dict]:
    prepost = weighted_average(rows, ("field", "post_type", "period"))
    lookup = {(r["field"], r["post_type"], r["period"]): r for r in prepost}
    out = []
    for field in sorted({r["field"] for r in prepost}):
        for post_type in ("question", "answer"):
            pre = lookup.get((field, post_type, "pre_chatgpt"))
            post = lookup.get((field, post_type, "post_chatgpt"))
            if not pre or not post:
                continue
            out.append(
                {
                    "field": field,
                    "post_type": post_type,
                    "pre_similarity": pre["book_similarity"],
                    "post_similarity": post["book_similarity"],
                    "delta_post_minus_pre": round(num(post, "book_similarity") - num(pre, "book_similarity"), 5),
                    "pre_word_count": pre["word_count"],
                    "post_word_count": post["word_count"],
                }
            )
    return out


def make_plots(output: Path, rows: list[dict]) -> None:
    import matplotlib.pyplot as plt

    monthly = weighted_average(rows, ("month", "post_type"))
    monthly = [row for row in monthly if row["month"] >= "2013-01"]
    by_type = defaultdict(list)
    for row in monthly:
        by_type[row["post_type"]].append(row)

    plt.figure(figsize=(10, 5))
    for post_type, values in sorted(by_type.items()):
        plt.plot([r["month"] for r in values], [num(r, "book_similarity") for r in values], label=post_type)
    plt.axvline(CHATGPT_MONTH, color="red", linestyle="--", label="ChatGPT")
    ticks = [r["month"] for i, r in enumerate(monthly) if i % 24 == 0]
    plt.xticks(ticks, rotation=45, ha="right")
    plt.ylabel("book-profile cosine similarity")
    plt.ylim(0.84, 0.93)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output / "book_similarity_over_time.png", dpi=160)
    plt.close()

    prepost = weighted_average(rows, ("period", "post_type"))
    labels = [f"{r['post_type']}\n{r['period'].replace('_', ' ')}" for r in prepost]
    plt.figure(figsize=(7, 4.5))
    plt.bar(labels, [num(r, "book_similarity") for r in prepost])
    plt.ylim(0.85, 0.92)
    plt.ylabel("book-profile cosine similarity")
    plt.tight_layout()
    plt.savefig(output / "book_similarity_pre_post.png", dpi=160)
    plt.close()

    deltas = field_pre_post_deltas(rows)
    fields = sorted({r["field"] for r in deltas})
    x = range(len(fields))
    plt.figure(figsize=(max(9, len(fields) * 0.75), 5))
    for offset, post_type in enumerate(("question", "answer")):
        values = [
            next((num(r, "delta_post_minus_pre") for r in deltas if r["field"] == field and r["post_type"] == post_type), 0)
            for field in fields
        ]
        plt.bar([i + offset * 0.4 for i in x], values, width=0.4, label=post_type)
    plt.axhline(0, color="black", linewidth=1)
    plt.xticks([i + 0.2 for i in x], fields, rotation=45, ha="right")
    plt.ylabel("post minus pre book-profile similarity")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output / "book_similarity_delta_by_field.png", dpi=160)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute Math.SE book-profile similarity over time.")
    parser.add_argument("--books", type=Path, default=DEFAULT_BOOKS)
    parser.add_argument("--mse", type=Path, default=DEFAULT_MSE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--min-words", type=int, default=50000)
    args = parser.parse_args()

    result = run_similarity(
        books_path=args.books,
        mse_path=args.mse,
        output=args.output,
        min_words=args.min_words,
    )

    print(f"Wrote {len(result['rows'])} monthly field/post-type similarity rows to {result['output_dir']}")

if __name__ == "__main__":
    main()
