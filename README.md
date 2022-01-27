NHSSS: Data Quality Team (Informatics)
========================================================================================================================
Author/s:

- [Palo, John Benedict](https://www.facebook.com/Bene.Palo/)

Team Members:

- [Palo, John Benedict](https://www.facebook.com/Bene.Palo/) _(Database Specialist)_
- [Rendon, Maeghan](https://www.facebook.com/megannnss/) _(Surveillance Officer)_
- [Asilo, Kathrina Michaela](https://www.facebook.com/kathrinamichaela/) _(Health Program Officer II)_

Data:

- [HIV/AIDS & ART Registry of the Philippines](https://doh.gov.ph/statistics)

---

Installation Instructions
-------------------------

1. Install PyCharm
2. Add VCS
3. Manage Remotes (origin = http://130.105.75.3:3000/nhsss_admin/ohasis-dqt.git)
4. Make your own branch (initials, lastname, .doh) jrpalo.doh
5. Pull

[To be added.]

---

Directory Structure
-------------------

### Parent-level

This project's directory structure was created to account for team collaboration in the management and wrangling of
OHASIS data.

```
DQT
├───archive
├───data
├───ref
└───src
```

**Primary Code Library**

- `src` - cotnains all related code used to generate any OHASIS-extracted dataset.

**Auto-Output Directories**

- `archive` - all input files are copied and cleaned into this directory during consolidation. Scripts, input and output
  files, and references are all automatically copied here.
    - _NOTE: Include files here in `.gitignore`_

**User Input Directories**

- `data` - various OHASIS output data to be placed here if needed in processing subsets. Takes output files from
  the `archive` directory.
    - _NOTE: Include files here in `.gitignore`_

- `ref` - contains various standard translations and corrections that can be done across any reporting month's
  consolidation.

### Second level

Three of the four sub-directories are stratified accordingly by reporting date, taking the format `yyyy.mm`. This is to
take into consideration each month's specific nuances with reporting and consolidation.

The files inside the `ref/.all` sub-directory are data corrections that are not specific to any reporting date, and are
thus part of the standard running for any reporting month/quarter.

```
DQT
├───archive
│   └───2021.08
│
├───data
│   └───2021.08
│
└───ref
    ├───.all
    └───2021.08

```

The `src` directory is divided into the different types of output files the scripts can generate. These have further
stratification to delineate between the different exported data they output.

- `official` - NHSSS final output datasets.

- `ohasis` - OHASIS first-level exports (Forms, Inventory, etc.)

```
DQT
└───src
    ├───official
    └───ohasis

```

---
