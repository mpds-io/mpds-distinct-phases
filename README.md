Pauling File Distinct Phases
==========

As known, the standard unit of the Pauling File (MPDS) data is an _entry_. All the entries are subdivided into three kinds: _crystalline structures_, _physical properties_, and _phase diagrams_. They are called _S_-, _P_- and _C_-entries, correspondingly. All these entries have persistent identifiers (similar to DOIs), e.g. [S377634](https://mpds.io/entry/S377634), [P600028](https://mpds.io/entry/P600028), or [C100027](https://mpds.io/entry/C100027).

These three kinds of entries are grouped together into the distinct phases they belong. The term _distinct phase_ is often used in the alloys description, however here we apply it for all the compounds known from the scientific literature. A tremendous work was done by the Pauling File team in the past 30 years to manually distinguish about 200 000 inorganic materials phases, appeared at least once in the literature. Each phase has a unique combination of (_a_) chemical formula, (_b_) space group, and (_c_) Pearson symbol. Each phase has the permanent integer identifier called _phase_id_.

This repository contains the yearly releases of the Pauling File distinct phases for all the known _unary_ and _binary_ compounds. Please [contact us](https://mpds.io/products) if you are interested in the other compounds.


## Technical details

The dumps in the `release` folders are in `JSON` format and have the following structure:

```json
[
	{
		"id": "https://mpds.io/phase_id/5019",
		"formula": {"short": "Ge", "full": "Ge cub"},
		"spg": 227,
		"pearson": "cF8",
		"entries": 1571,
		"articles": 748
	},
...
]
```

The field `id` is the permanent URL of the particular distinct phase at the MPDS platform. Its last integer part is the _phase_id_.
The short and full `formula` stand for the terse plain-text and detailed HTML description of the chemical composition, respectively.
The `spg` is the space group number.
The `pearson` is the Pearson symbol (note its numeric part which is a number of atoms in the standard crystalline unit cell).
The `entries` is the number of the entries at the MPDS platform at the year of the release. Note these are only the `peer-reviewed` class of entries, not the `machine learning` or `ab initio calculations`. In the MPDS API the `peer-reviewed` class is referenced by `dtype` parameter equal to `1` (or `MPDSDataTypes.PEER_REVIEWED`). In the MPDS GUI this is given by the search keyword `peer-reviewed`.
The `articles` is the number of peer-reviewed literature sources containing the particular distinct phase processed by the Pauling File team at the year of the release.


## License

Copyright 2022 Materials Phases Data System (Switzerland), NIMS Government Agency (Japan), and Materials Platform for Data Science (Estonia).

All rights reserved.

Academic usage is allowed.

Please [contact us](https://mpds.io/products) if you would like to use these data in the for-profit purposes or if you are interested in the full dataset.
