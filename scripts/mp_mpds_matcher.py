"""
Match all available IDs from Materials Project with IDs from MPDS.
Result file will be saved in the same directory from which the script is launched.
"""

import os.path
import time
from pathlib import Path
from typing import Union

import polars as pl
import ujson as json
from mp_api.client import MPRester
from mpds_client import APIError, MPDSDataRetrieval, MPDSDataTypes


def mpds_downloader():
    """
    Request atomic from MPDS structures and save in file
    """
    print("---Started receiving: atomic structure")

    client = MPDSDataRetrieval(dtype=MPDSDataTypes.ALL)
    client.chilouttime = 1.0

    fp = open(os.path.join("./atomic_structures.jsonl"), "w")
    for year in range(1890, 2025):
        time.sleep(1.0)
        try:
            for entry in client.get_data(
                {"props": "atomic structure", "years": str(year)}, fields={}
            ):
                fp.write(json.dumps(entry, escape_forward_slashes=False) + "\n")
        except APIError as error:
            if error.code == 204:
                continue  # No hits
            else:
                raise
    fp.close()
    print("---Successfully saved to atomic_structures.jsonl")


def matcher_mp_mpds(
    mpds_file_path: Union[bool, str],
    formulae: list,
    sg: list,
    mp_ids: list,
):
    """
    Get phase_id from file or MPDS by client. Match ID from Materials Project and MPDS
    by formula and space group

    Parameters
    ----------
    mpds_file_path: Path
        Path to jsonl file with keys: 'phase_id', 'formula', 'symmetry'
    formulae : list
        Formulas from MP
    sg: list
        Symmetry (space group) from MP
    mp_ids: list
        IDs from MP

    Returns
    -------
    pl.DataFrame
        Consist of columns: 'phase_id' (ID from MPDS), 'formula', 'symmetry', 'ID_mp' (ID from MP)
    """
    if mpds_file_path:
        if Path(mpds_file_path).exists():
            mpds_df = pl.read_ndjson(mpds_file_path)
            print("Data with ID from MPDS found in directory. Start matches!")

    # run requests to MPDS
    else:
        print("Data with ID from MPDS not found in directory. Start requests!")

        mpds_downloader()
        mpds_df = pl.read_ndjson("./atomic_structures.jsonl")
        mpds_df = mpds_df.select(["phase_id", "chemical_formula", "sg_n"])
        mpds_df = mpds_df.rename(
            {"phase_id": "phase_id", "chemical_formula": "formula", "sg_n": "symmetry"}
        )

    mp_df = pl.DataFrame(
        {
            "ID_mp": mp_ids,
            "formula": formulae,
            "symmetry": sg,
        }
    ).with_columns(pl.col("symmetry").cast(pl.Int64))

    matched_data = mpds_df.join(
        mp_df,
        left_on=["formula", "symmetry"],
        right_on=["formula", "symmetry"],
        how="inner",
    )

    print("Matches by formula and Space group found:", len(matched_data))
    return matched_data


def mp_downloader(mp_path: str, api_key: str) -> pl.DataFrame:
    """
    Download all available mp-ids, symmetry, chemical formulae.
    Save in json format
    """
    try:
        dfrm = pl.read_json(mp_path + "/all_id_mp.json")
        print("All IDs, symmetry, formulae already present in directory")
    except:
        client = MPRester(api_key)
        ans_ids, ans_formula, ans_sg = [], [], []
        answer = client.summary.search(
            fields=["material_id", "formula_pretty", "symmetry"]
        )
        [
            (
                ans_ids.append(str(i.material_id)),
                ans_formula.append(i.formula_pretty),
                ans_sg.append(str(i.symmetry.number)),
            )
            for i in answer
        ]
        dfrm = pl.DataFrame(
            {"identifier": ans_ids, "formula": ans_formula, "symmetry": ans_sg},
            schema=["identifier", "formula", "symmetry"],
        )
        dfrm.write_json(mp_path + "/all_id_mp.json")
    return dfrm


def id_mp_mpds_matcher(
    mp_path: str,
    mp_api_key: str,
    mpds_file_path: Union[bool, Path] = False,
) -> pl.DataFrame:
    """
    Run request all IDs from Materials Project, then match it with IDs from MPDS

    Parameters
    ----------
    mp_path : Path
        Path to directory for store data from Materials Project
    mp_api_key: str
        Key from Materials Project account
    mpds_file_path: Path, optional
        Path to jsonl file with keys: 'phase_id', 'formula', 'symmetry'

    Returns
    -------
    dfrm: pl.DataFrame with columns: 'phase_id', 'formula', 'symmetry', 'ID_mp'
    """
    mp_downloader(mp_path, mp_api_key)

    if Path("id_match.json").exists():
        dfrm = pl.read_json(mp_path + "/id_match.json")
        print(f"'id_match.json' already in directory. Size: {len(dfrm)}")
        return dfrm
    else:
        mp_dfrm = pl.read_json(mp_path + "/all_id_mp.json")
        dfrm = matcher_mp_mpds(
            mpds_file_path,
            sg=list(mp_dfrm["symmetry"]),
            formulae=list(mp_dfrm["formula"]),
            mp_ids=list(mp_dfrm["identifier"]),
        )
        dfrm.write_json(mp_path + "/id_match.json")
    return dfrm


if __name__ == "__main__":
    mp_path = "./"
    mp_api_key = "YOUR_API_KEY"

    id_mp_mpds_matcher(mp_path, mp_api_key)
