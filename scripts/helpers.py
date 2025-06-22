
from collections import defaultdict
import json

import polars as pl


def read_jsonl(file_path):
    # mb change on polars dfrm?
    with open(file_path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f]


def group_by_phase(data):
    grouped = defaultdict(set)
    for entry in data:
        if 'phase' in entry and 'phase_id' in entry:
            grouped[entry['phase']].add(entry['phase_id'])
    return grouped


def fetch_duplicates(grouped_data):
    return [
        {'phase': phase, 'phase_ids': list(phase_ids)}
        for phase, phase_ids in grouped_data.items()
        if len(phase_ids) > 1
    ]


def find_duplicates_mpds(input_path, output_path):
    data = read_jsonl(input_path)
    grouped = group_by_phase(data)
    duplicates = fetch_duplicates(grouped)

    with open(output_path, 'w', encoding='utf-8') as f:
        for item in duplicates:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


def find_duplicates_mp(input_path: str):
    df = pl.read_json(input_path)

    grouped = (
        df.group_by(["formula", "symmetry", "pearson"])
        .agg(pl.col("identifier").alias("identifiers"))
        .filter(pl.col("identifiers").list.len() > 1)
    )

    grouped = grouped.with_columns(
        pl.col("identifiers").list.len().alias("count")
    )

    with open("./duplicate_mp.jsonl", "w") as f:
        for row in grouped.iter_rows(named=True):
            f.write(json.dumps(row) + "\n")

    print("Saved into ./phase_duplicates.json")
