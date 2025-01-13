"""
Match all available IDs from Materials Project with IDs from MPDS
"""

import json
from typing import Optional, Union

import periodictable
import polars as pl
from mp_api.client import MPRester
from mpds_client import MPDSDataRetrieval
from pymatgen.core import Composition


def get_composition(formulae: list, num_el_from: int = 6) -> list:
    """
    Get composition from chemical formula

    Parameters
    ----------
    formulae: list
        List of formulae from MP
    num_el_from: int, optional
        Number of different elements in a chemical formula that should be included in result list
    """
    comp = []
    for f in formulae:
        temp_comp = sorted([i for i in Composition(f).get_el_amt_dict().keys()])
        if temp_comp not in comp:
            comp.append(temp_comp)
    comp = [i for i in comp if len(i) >= num_el_from]

    return comp


def define_unary(formulae: list) -> list:
    """
    Define set of formulae for unary classes

    Parameters
    ----------
    formulae: list
        List of formulae from MP

    Returns
    -------
    unary: list
        List of unary formulae which present in MP
    """
    unary = []
    for f in formulae:
        if len([i for i in Composition(f).get_el_amt_dict().keys()]) == 1:
            unary.append([i for i in Composition(f).get_el_amt_dict().keys()][0])
    return set(unary)


def mpds_downloader(api_key: str, formulae: list, unary: set):
    """
    Download phase_ids for "unary", "binary", "ternary", "quaternary", "quinary" classes.
    After this, results with sets of elements greater than 5 are specifically requested.
    Unary ones are requested based on the formulae presented in MP, remaining 4 classes
    are all based on the 188 existing elements.

    Parameters
    ----------
    api_key: str
        API key for MPDS
    formulae: list
        Formulas from MP database
    unary: set
        Set of unary elements, which are included in formulae from MP
    """
    mpds_ids = []
    elements = [
        element.symbol for element in periodictable.elements if element is not None
    ]
    classes = ["unary", "binary", "ternary", "quaternary", "quinary"]

    client = MPDSDataRetrieval(dtype=7, api_key=api_key)

    for el in elements:
        print(f'PROCESSING: {el}, iteration: {elements.index(el)}')
        for cl in classes:
            if classes == "unary":
                if el not in unary:
                    continue
            try:
                client.chillouttime = 2
                ans = client.get_data({"elements": el, "classes": cl})
                for row in ans:
                    if row[:3] not in mpds_ids:
                        mpds_ids.append(row[:3])
            except Exception as e:
                print(e)
        if elements.index(el) % 15 == 0:
            pl.DataFrame(mpds_ids, ["phase_id", "formula", "symmetry"]).write_csv(
                f"mpds_IDs_it_{elements.index(el)}.csv"
            )

    pl.DataFrame(mpds_ids, ["phase_id", "formula", "symmetry"]).write_csv(
        "mpds_IDs_quinary.csv"
    )

    print(f'PROCESSING: start requesting more then quinary')
    more_then_quinary = [i[:2] for i in get_composition(formulae, 6)]
    more_then_quinary_cat = [
        list(item)
        for item in set(tuple(sorted(sublist)) for sublist in more_then_quinary)
    ]

    cnt = 0
    for el in more_then_quinary_cat:
        client.chillouttime = 2
        try:
            cnt += 1
            print(f'PROCESSING: iteration: {cnt}, from {len(more_then_quinary_cat)}')
            ans = client.get_data({"elements": "-".join(el)})
            for i, row in enumerate(ans):
                if row[:3] not in mpds_ids:
                    mpds_ids.append(row[:3])
        except Exception as e:
            print(e)
    pl.DataFrame(mpds_ids, ["phase_id", "formula", "symmetry"]).write_csv(
        "./mpds_IDs_ready.csv"
    )
    return mpds_ids


def matcher_mp_mpds(
    mpds_id_path: Union[bool, str],
    formulae: list,
    sg: list,
    mp_ids: list,
    api_key: Union[bool, str] = False,
):
    """
    Get phase_id from file or MPDS by client. Match ID from Materials Project and MPDS
    by formula and space group

    Parameters
    ----------
    mpds_id_path: Union[bool, str], optional
        Path to json file with all phase-IDs from MPDS. By default, == False,
        in this case phase-IDs from MPDS will be requested
    formulae : list
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
        Consist of columns: 'phase_id' (ID from MPDS), 'formula', 'symmetry', 'ID_mp' (ID from MP)
    """
    phase_ids = []

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
        for i in range(len(formulae)):
            key_full = (formulae[i], int(sg[i]))
            key_short = (formulae[i], int(sg[i]))

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
        try:
            mpds_df = pl.read_csv("./mpds_IDs_ready.csv")
            print("Data with ID from MPDS found in directory. Start matches!")
        except:
            print("Data with ID from MPDS not found in directory. Start requests!")
            unary = define_unary(formulae)

            mpds_df = pl.DataFrame(
                mpds_downloader(api_key, formulae, unary),
                schema=["phase_id", "formula", "symmetry"],
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
        dfrm = pl.read_json(mp_path + "all_id_mp.json")
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
    dfrm: pl.DataFrame with columns: 'phase_id', 'formula', 'symmetry', 'ID_mp'

    """
    mp_downloader(mp_path, mp_api_key)
    try:
        dfrm = pl.read_json(mp_path + "id_match.json")
        print(f"'id_match.json' already in directory. Size: {len(dfrm)}")
        return dfrm
    except:
        mp_dfrm = pl.read_json(mp_path + "all_id_mp.json")
        dfrm = matcher_mp_mpds(
            mpds_id_path,
            sg=list(mp_dfrm["symmetry"]),
            formulae=list(mp_dfrm["formula"]),
            mp_ids=list(mp_dfrm["identifier"]),
            api_key=mpds_api_key,
        )
        dfrm.write_json(mp_path + "id_match.json")
    return dfrm


if __name__ == "__main__":
    mp_path = "./data/mp"
    mpds_api_key = ""
    mp_api_key = ""

    assert os.path.exists(mp_path), mp_path

    id_mp_mpds_matcher(mp_path, mp_api_key, mpds_id_path, mpds_api_key)
