"""
Match all available IDs from Materials Project with IDs from MPDS
"""

import json
from typing import Optional, Union

import polars as pl
from mp_api.client import MPRester
from mpds_client import MPDSDataRetrieval


def mpds_request(
    mpds_id_path: Union[bool, str],
    formulas: list,
    sg: list,
    mp_ids: list,
    api_key: Union[bool, str] = False,
):
    """
    Get phase_id from file or MPDS by client. Match ID from Materials Project and MPDS by formula and space group

    Parameters
    ----------
    mpds_id_path: Union[bool, str], optional
        Path to json file with all phase-IDs from MPDS. By default, == False,
        in this case phase-IDs from MPDS will be requested
    formulas : list
        Formulas from MP
    sg: list
        Symmetry (space group) from MP
    mp_ids: list
        IDs from MP
    api_key: Union[bool, str], optional
        Key from MPDS account

    Returns
    -------
    pl.DataFrame
        Consist of columns: 'phase_id' (id from MPDS), 'identifier' (id from MP), symmetry, formula
    """
    phase_ids = []
    found, loss = 0, 0

    if mpds_id_path:
        with open(mpds_id_path, "r") as file:
            data = json.load(file)

        # Dict for fast search, with formula and space group
        full_formula_dict = {}
        short_formula_dict = {}
        for row in data:
            key_full = (row["formula"]["full"], row["spg"])
            key_short = (row["formula"]["short"], row["spg"])

            if key_full not in full_formula_dict:
                full_formula_dict[key_full] = row["id"].split("/")[-1]

            if key_short not in short_formula_dict:
                short_formula_dict[key_short] = row["id"].split("/")[-1]

        # Search match
        for i in range(len(formulas)):
            key_full = (formulas[i], int(sg[i]))
            key_short = (formulas[i], int(sg[i]))

            if (
                key_full in full_formula_dict.keys()
                and [full_formula_dict[key_full], mp_ids[i], key_full[0], key_full[1]]
                not in phase_ids
            ):
                phase_ids.append(
                    [full_formula_dict[key_full], mp_ids[i], key_full[0], key_full[1]]
                )
            elif (
                key_short in short_formula_dict
                and [
                    short_formula_dict[key_short],
                    mp_ids[i],
                    key_short[0],
                    key_short[1],
                ]
                not in phase_ids
            ):
                phase_ids.append(
                    [
                        short_formula_dict[key_short],
                        mp_ids[i],
                        key_short[0],
                        key_short[1],
                    ]
                )

        print("Found matches:", len(phase_ids))
        return pl.DataFrame(
            phase_ids, schema=["phase_id", "identifier", "formula", "symmetry"]
        )

    # run requests to MPDS
    else:
        print("Raw data with MPDS phase_ids not found in directory. Start requests!")
        client = MPDSDataRetrieval(dtype=7, api_key=api_key)
        for i in range(len(formulas)):
            try:
                client.chillouttime = 2
                ans = client.get_data({"sgs": sg[i], "formulae": formulas[i]})
                phase_ids.append([str(ans[0][0]), mp_ids[i]])
                found += 1
            except Exception as e:
                print(e)
                if e != "HTTP error code 204: No Results (No hits)":
                    client.chillouttime += 1
                loss += 1
                print("Not found:", loss)

        print("Matches by formula and Space group found:", found)
        return pl.DataFrame(phase_ids, schema=["phase_id", "identifier"])


def finding_matches_id_by_formula_sg(
    mp_path: str, mpds_api_key: str, mpds_id_path: Union[bool, str] = False
) -> pl.DataFrame:
    """
    Find 'phase_id' for material from MPDS by formula, space group from Materials Project.
    Save answer in JSON-format

    Parameters
    ----------
    mp_path : str
        Math to directory for store data from Materials Project
    mpds_api_key: srt
        Key from MPDS account
    mpds_id_path: Union[bool, str], optional
        Path to json file with all phase-IDs from MPDS. By default, == False,
        in this case phase-IDs from MPDS will be requested

    Returns
    -------
    dfrm: pl.DataFrame
        Consist of columns: 'phase_id' (id from MPDS), 'identifier' (id from Materials Project), symmetry, formula
    """
    try:
        dfrm = pl.read_json(mp_path + "id_match.json")
        print(f"'id_match.json' already in directory. Size: {len(dfrm)}")
        return dfrm
    except:
        mp_dfrm = pl.read_json(mp_path + "all_id_mp.json")
        dfrm = mpds_request(
            mpds_id_path,
            sg=list(mp_dfrm["symmetry"]),
            formulas=list(mp_dfrm["formula"]),
            mp_ids=list(mp_dfrm["identifier"]),
            api_key=mpds_api_key,
        )
        dfrm.write_json(mp_path + "id_match.json")
        return dfrm


def mp_request(mp_path: str, api_key: str) -> pl.DataFrame:
    """
    Request all available mp-ids, symmetry, chemical formulas.
    Save in json format
    """
    try:
        dfrm = pl.read_json(mp_path + "all_id_mp.json")
        print("All IDs, symmetry, formulas already present in directory")
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
        dfrm.write_json(mp_path + "all_id_mp.json")
    return dfrm


def id_mp_mpds_matcher(
    mp_path: str,
    mp_api_key: str,
    mpds_id_path: Union[bool, str] = False,
    mpds_api_key: Optional[Union[bool, str]] = None,
) -> pl.DataFrame:
    """
    Run request all IDs from Materials Project, then match it with IDs from MPDS

    Parameters
    ----------
    mp_path : str
        Math to directory for store data from Materials Project
    mp_api_key: str
        Key from Materials Project account
    mpds_id_path: Union[bool, str], optional
        Path to json file with all phase-IDs from MPDS. By default, == False,
        in this case phase-IDs from MPDS will be requested
    mpds_api_key: Union[bool, str], optional
        Key from MPDS account

    Returns
    -------
    dfrm: pl.DataFrame
    """
    mp_request(mp_path, mp_api_key)
    dfrm = finding_matches_id_by_formula_sg(mp_path, mpds_api_key, mpds_id_path)
    return dfrm


if __name__ == "__main__":
    mp_path = "./data/mp_database/"
    mpds_id_path = "./data/raw_mpds/mpds_phases_jan2024.json"
    mpds_api_key = "KEY"
    mp_api_key = "KEY"

    id_mp_mpds_matcher(mp_path, mp_api_key, mpds_id_path, mpds_api_key)
