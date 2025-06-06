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
from pymatgen.core import Lattice, Structure
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from dup_mpds import find_duplicates_mpds
from dup_mp import find_duplicates_mp


def parse_structure(data):
    """Create pymatgen Structure from MPDS data in string format"""
    lines = [line.strip() for line in data.split("\n") if line.strip()]

    abc_line = next(line for line in lines if line.startswith("abc   :"))
    abc = list(map(float, abc_line.split(":")[1].split()[:3]))

    angles_line = next(line for line in lines if line.startswith("angles:"))
    angles = list(map(float, angles_line.split(":")[1].split()[:3]))

    atoms = []
    for line in lines:
        if line[0].isdigit():
            parts = line.split()
            element = parts[1]
            a, b, c = map(float, parts[2:5])
            atoms.append((element, [a, b, c]))

    lattice = Lattice.from_parameters(*abc, *angles)
    species = [atom[0] for atom in atoms]
    positions = [atom[1] for atom in atoms]

    return Structure(lattice, species, positions)


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
    mp_n_atoms: list,
):
    """
    Get phase_id from file or MPDS by client. Match ID from Materials Project and MPDS
    by formula, space group, number of atom in primitive cell.

    Parameters
    ----------
    mpds_file_path: Path
        Path to jsonl file with keys: 'phase_id', 'formula', 'symmetry', 'n_atoms'
    formulae : list
        Formulas from MP
    sg: list
        Symmetry (space group) from MP
    mp_ids: list
        IDs from MP
    mp_n_atoms: list
        Number of atoms in primitive cell from MP

    Returns
    -------
    pl.DataFrame
        Consist of columns: 'phase_id' (ID from MPDS), 'formula', 
        'symmetry', 'n_atoms', 'ID_mp' (ID from MP)
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
        mpds_df = mpds_df.rename(
            {"phase_id": "phase_id", "chemical_formula": "formula", "sg_n": "symmetry"}
        )
        n_atoms_mpds = [
            parse_pearson(i)[-1] for _, i in enumerate(mpds_df["phase"].to_list())
        ]
        mpds_df = mpds_df.with_columns(pl.Series(name="n_atoms", values=n_atoms_mpds))
        mpds_df = mpds_df.select(["phase_id", "formula", "symmetry", "n_atoms"])
        print(mpds_df.columns)

    mp_df = pl.DataFrame(
        {
            "ID_mp": mp_ids,
            "formula": formulae,
            "symmetry": sg,
            "n_atoms": mp_n_atoms,
        }
    ).with_columns(pl.col("symmetry").cast(pl.Int64))

    matched_data = mpds_df.join(
        mp_df,
        left_on=["formula", "symmetry", "n_atoms"],
        right_on=["formula", "symmetry", "n_atoms"],
        how="inner",
    )
    matched_data = matched_data.unique(maintain_order=True)

    print("Matches by formula and Space group found:", len(matched_data))
    return matched_data


def mp_prepeare_structure(mp_path: str):
    dfrm = pl.read_json(mp_path + "/all_id_mp_upd.json")
    ans_ids = dfrm["identifier"].to_list()
    ans_formula = dfrm["formula"].to_list()
    ans_sg = dfrm["symmetry"].to_list()
    ans_structure = dfrm["pearson"].to_list()
    n_atoms = [
        len(SpacegroupAnalyzer(parse_structure(i)).get_primitive_standard_structure())
        for i in ans_structure
    ]
    dfrm = pl.DataFrame(
        {
            "identifier": ans_ids,
            "formula": ans_formula,
            "symmetry": ans_sg,
            "strucrure": ans_structure,
            "pearson": n_atoms,
        },
        schema=["identifier", "formula", "symmetry", "strucrure", "pearson"],
    )
    # save prepared data
    dfrm.write_json(mp_path + "/all_id_mp_upd.json")


def mp_downloader(mp_path: str, api_key: str) -> pl.DataFrame:
    """
    Download all available mp-ids, symmetry, chemical formulae.
    Save in json format
    """
    try:
        dfrm = pl.read_json(mp_path + "/all_id_mp_upd.json")
        if len(dfrm.columns) != 5:
            print(
                "File 'all_id_mp_upd.json' does not contain all required columns, stert processing structure!"
            )
            mp_prepeare_structure(mp_path)
        print("All IDs, symmetry, formulae already present in directory")

    except:
        client = MPRester(api_key)
        ans_ids, ans_formula, ans_sg, ans_structure = [], [], [], []

        answer = client.summary.search(
            fields=["material_id", "formula_pretty", "symmetry", "pearson"]
        )
        [
            (
                ans_ids.append(str(i.material_id)),
                ans_formula.append(i.formula_pretty),
                ans_sg.append(str(i.symmetry.number)),
                ans_structure.append(str(i.structure)),
            )
            for i in answer
        ]
        dfrm = pl.DataFrame(
            {
                "identifier": ans_ids,
                "formula": ans_formula,
                "symmetry": ans_sg,
                "pearson": ans_structure,
            },
            schema=["identifier", "formula", "symmetry", "pearson"],
        )
        # save row data
        dfrm.write_json(mp_path + "/all_id_mp.json")
        # prepare structure, define n_atoms
        mp_prepeare_structure(mp_path)
    return dfrm


def parse_pearson(symb):
    lat, nat = "", ""
    try:
        symb = symb.split("/")[-1]
        for s in symb:
            if s.isdigit():
                nat += s
            else:
                lat += s
        return lat, int(nat) if nat else 0
    except:
        return lat, 0


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
        Path to directory for store data from Materials Project. 
        Columns: 'identifier', 'formula', 'symmetry', 'strucrure', 'pearson'
    mp_api_key: str
        Key from Materials Project account
    mpds_file_path: Path, optional
        Path to jsonl file with keys: 'phase_id', 'formula', 'symmetry'

    Returns
    -------
    dfrm: pl.DataFrame with columns: 'phase_id', 'formula', 'symmetry', 'n_atoms', 'ID_mp'
    """
    mp_downloader(mp_path, mp_api_key)

    if Path("id_match.json").exists():
        dfrm = pl.read_json(mp_path + "/id_match.json")
        print(f"'id_match.json' already in directory. Size: {len(dfrm)}")
        return dfrm
    else:
        mp_dfrm = pl.read_json(mp_path + "/all_id_mp_upd.json")
        dfrm = matcher_mp_mpds(
            mpds_file_path,
            sg=list(mp_dfrm["symmetry"]),
            formulae=list(mp_dfrm["formula"]),
            mp_ids=list(mp_dfrm["identifier"]),
            mp_n_atoms=list(mp_dfrm["pearson"]),
        )
        dfrm.write_json(mp_path + "/id_match.json")
    return dfrm


if __name__ == "__main__":
    mp_path = "./"
    # set False if you want to download MPDS data
    mpds_file_path = False
    mp_api_key = "HERE_YOUR_API_KEY"

    # id_mp_mpds_matcher(mp_path, mp_api_key, mpds_file_path)
    find_duplicates_mpds(input_path="./atomic_structures.jsonl", output_path="./duplicate_mpds.jsonl")
    find_duplicates_mp(input_path="./all_id_mp_upd.json")