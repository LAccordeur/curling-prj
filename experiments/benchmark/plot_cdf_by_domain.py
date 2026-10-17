#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


DEFAULT_METRIC = "completion_latency_s"
DEFAULT_GROUP_BY = "domain_id"
PERCENTILES = [50, 95]
METRIC_LABELS = {
    "admission_latency_s": "Admission latency (second)",
    "completion_latency_s": "Completion latency (second)",
    "operation_latency_s": "Operation latency (second)",
    "upload_latency_s": "Upload latency (second)",
}
GROUP_LABELS = {
    "domain_id": "domain",
    "object_size_label": "object size",
    "object_size_bytes": "object size",
}


def numeric(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def load_by_group(csv_path, metric, group_by):
    by_group = {}
    skipped = 0
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        if metric not in fieldnames:
            raise ValueError(f"CSV does not contain metric column: {metric}")
        if group_by not in fieldnames:
            raise ValueError(f"CSV does not contain group-by column: {group_by}")

        for row in reader:
            value = numeric(row.get(metric))
            if value is None:
                skipped += 1
                continue
            group = row.get(group_by) or "unknown"
            if group_by == "object_size_bytes":
                group = format_size_label(group)
            by_group.setdefault(group, []).append(value)

    return by_group, skipped


def load_by_domain(csv_path, metric):
    return load_by_group(csv_path, metric, "domain_id")


def load_by_object_size(csv_path, metric, size_column="object_size_label"):
    return load_by_group(csv_path, metric, size_column)


def cdf_points(values):
    ordered = sorted(values)
    count = len(ordered)
    y_values = [(index + 1) / count for index in range(count)]
    return ordered, y_values


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * pct / 100)
    return ordered[index]


def percentile_summary(values):
    return {f"p{pct}": percentile(values, pct) for pct in PERCENTILES}


def fmt(value):
    if value is None:
        return ""
    return f"{value:.2f}"


def metric_label(metric):
    if metric in METRIC_LABELS:
        return METRIC_LABELS[metric]
    return metric.replace("_", " ").title()


def group_label(group_by):
    if group_by in GROUP_LABELS:
        return GROUP_LABELS[group_by]
    return group_by.replace("_", " ")


def format_size_label(value):
    size = numeric(value)
    if size is None:
        return value or "unknown"
    units = [
        (1024 ** 3, "GB"),
        (1024 ** 2, "MB"),
        (1024, "KB"),
    ]
    for factor, suffix in units:
        if size >= factor:
            amount = size / factor
            if amount.is_integer():
                return f"{int(amount)} {suffix}"
            return f"{amount:.1f} {suffix}"
    return f"{int(size)} B"


def annotation_candidates(percentile):
    if percentile >= 90:
        return [
            (6, -10, "left"),
            (-6, -10, "right"),
            (6, -24, "left"),
            (-6, -24, "right"),
            (6, 6, "left"),
            (-6, 6, "right"),
        ]
    return [
        (6, 6, "left"),
        (-6, 6, "right"),
        (6, -12, "left"),
        (-6, -12, "right"),
        (6, 20, "left"),
        (-6, 20, "right"),
    ]


def place_annotation(fig, ax, placed_bboxes, text, xy, color, percentile):
    renderer = None
    for dx, dy, horizontal_alignment in annotation_candidates(percentile):
        annotation = ax.annotate(
            text,
            xy=xy,
            xytext=(dx, dy),
            textcoords="offset points",
            color=color,
            fontsize=10,
            fontweight="bold",
            ha=horizontal_alignment,
            bbox={
                "boxstyle": "round,pad=0.18",
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.72,
            },
        )
        fig.canvas.draw()
        renderer = renderer or fig.canvas.get_renderer()
        bbox = annotation.get_window_extent(renderer=renderer).expanded(1.04, 1.12)
        if not any(bbox.overlaps(existing) for existing in placed_bboxes):
            placed_bboxes.append(bbox)
            return
        annotation.remove()

    annotation = ax.annotate(
        text,
        xy=xy,
        xytext=(8, 8),
        textcoords="offset points",
        color=color,
        fontsize=10,
        fontweight="bold",
        bbox={
            "boxstyle": "round,pad=0.18",
            "facecolor": "white",
            "edgecolor": "none",
            "alpha": 0.72,
        },
    )
    fig.canvas.draw()
    renderer = renderer or fig.canvas.get_renderer()
    placed_bboxes.append(annotation.get_window_extent(renderer=renderer).expanded(1.04, 1.12))


def plot_cdf(by_group, metric, output_path, title="", xscale="linear", group_by=DEFAULT_GROUP_BY):
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required. Install it with: pip install matplotlib") from exc

    if not by_group:
        raise ValueError("No numeric records found to plot.")

    plt.rcParams.update({
        "font.family": "DejaVu Serif",
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "font.size": 16,
        "axes.titlesize": 16,
        "axes.labelsize": 16,
        "xtick.labelsize": 15,
        "ytick.labelsize": 15,
        "legend.fontsize": 15,
    })

    fig, ax = plt.subplots(figsize=(4.0, 3.6))
    placed_bboxes = []
    for group, values in sorted(by_group.items()):
        if not values:
            continue
        x_values, y_values = cdf_points(values)
        percentiles = percentile_summary(values)
        label = group
        line = ax.plot(x_values, y_values, linewidth=3.0, label=label)[0]
        color = line.get_color()
        for pct in PERCENTILES:
            value = percentiles[f"p{pct}"]
            if value is None:
                continue
            ax.axvline(value, color=color, linestyle="--", linewidth=1.3, alpha=0.45)
            ax.scatter(
                [value],
                [pct / 100],
                color=color,
                edgecolor="white",
                linewidth=0.9,
                s=70,
                zorder=4,
            )
            place_annotation(
                fig,
                ax,
                placed_bboxes,
                f"p{pct}={fmt(value)}",
                (value, pct / 100),
                color,
                pct,
            )

    ax.set_xlabel(metric_label(metric))
    ax.set_ylabel("CDF")
    ax.set_ylim(0, 1.01)
    ax.grid(True, linestyle="--", linewidth=0.8, alpha=0.5)
    if xscale != "linear":
        ax.set_xscale(xscale)
    group_count = sum(1 for values in by_group.values() if values)
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.30),
        ncol=max(1, min(group_count, 3)),
        frameon=True,
        framealpha=0.92,
        edgecolor="#cccccc",
        borderaxespad=0.0,
    )
    if title:
        ax.set_title(title)
    for spine in ax.spines.values():
        spine.set_linewidth(1.1)
    fig.tight_layout(rect=(0, 0, 1, 0.96))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot a benchmark latency CDF grouped by domain or object size.")
    parser.add_argument("csv", help="Benchmark CSV produced by run_benchmark.py")
    parser.add_argument("--metric", default=DEFAULT_METRIC, help=f"Numeric CSV column to plot. Default: {DEFAULT_METRIC}")
    parser.add_argument(
        "--group-by",
        choices=["domain_id", "object_size_label", "object_size_bytes"],
        default=DEFAULT_GROUP_BY,
        help="CSV column used as CDF series. Use object_size_label or object_size_bytes to group by data size.",
    )
    parser.add_argument("--output", default="", help="Output image path. Default: <csv-stem>.<metric>.cdf-by-<group>.pdf")
    parser.add_argument("--title", default="", help="Optional plot title.")
    parser.add_argument("--xscale", choices=["linear", "log"], default="linear")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    default_name = f"{csv_path.stem}.{args.metric}.cdf-by-{args.group_by}.pdf"
    output_path = Path(args.output) if args.output else csv_path.with_name(default_name)
    by_group, skipped = load_by_group(csv_path, args.metric, args.group_by)
    plot_cdf(by_group, args.metric, output_path, title=args.title, xscale=args.xscale, group_by=args.group_by)

    label = group_label(args.group_by)
    counts = ", ".join(f"{group}={len(values)}" for group, values in sorted(by_group.items()))
    print(f"wrote {output_path}")
    print(f"{label}s: {counts}")
    print("percentiles:")
    for group, values in sorted(by_group.items()):
        percentiles = percentile_summary(values)
        print(
            f"  {group}: "
            f"p50={fmt(percentiles['p50'])} "
            f"p95={fmt(percentiles['p95'])}"
        )
    if skipped:
        print(f"skipped non-numeric rows: {skipped}")


if __name__ == "__main__":
    main()
