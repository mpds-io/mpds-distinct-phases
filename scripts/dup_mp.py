import polars as pl
import json

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

if __name__ == "__main__":
    input_path = "./all_id_mp_upd.json"
    find_duplicates_mp(input_path)