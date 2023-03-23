# Schematic DB

This package is used to manage backends in the FAIR data flow.

It is assumed that manifests are stored in a project at [Synapse](https://www.synapse.org/), and have been validated via [Schematic](https://github.com/Sage-Bionetworks/schematic), or the [Data Curator App](dca.app.sagebionetworks.org).

## Local Development

### Setup

Clone the `schematic_db` package repository.

```bash
git clone https://github.com/Sage-Bionetworks/schematic_db.git
```

Install `poetry` (version 1.2 or later) using either the [official installer](https://python-poetry.org/docs/#installing-with-the-official-installer) or [pipx](https://python-poetry.org/docs/#installing-with-pipx). If you have an older installation of Poetry, we recommend uninstalling it first.

Start the virtual environment by doing:

```bash
poetry shell
```

Install the dependencies by doing:

```bash
poetry install --all-extras
```

### Tests

#### Secrets

Create a secrets file from template:

```bash
cp tests/data/example_secrets.yml tests/data/secrets.yml
```

tests/data/secrets.yml is in the .gitignore file so it can not be commited by git. There are sections for each type of database. Update the section(s) that are relevant to the code you will be working on.

#### Formatting tests

Before making a pull request you will want to make sure sure your code is formatted the correct way. Github will run the following tests:

```bash
mypy --disallow-untyped-defs .
black .
pylint schematic_db/* tests/*
```

You will want to make sure any changes passes the above tests before making a pull request.

#### Unit tests

Before making a pull request you will want to make sure sure your changes haven't broken any existing tests. The github workflow will do:

```bash
pytest
```

### Architecture

#### Documentation

Schematic db uses pdoc to generate documentation from docstrings. This exists [here](https://sage-bionetworks.github.io/schematic_db/schematic_db.html)

#### Classes

##### Schema

The [Schema](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/schema/schema.py) class is used to interact with the Schematic API to create a generic representation of the database from the schema. This is stored as a [DBConfig](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/db_config/db_config.py) object. The Schema is used by the [RDBBuilder](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_builder/rdb_builder.py) class.

##### Manifest Store

The [ManifestStore](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/manifest_store/manifest_store.py) class is used to interact with the Schematic API to download manifests needed to update the database. The ManifestStore is used by the [RDBUpdater](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/rdb_updater/rdb_updater.py) class.

##### Schema Graph

The [SchemaGraph](https://github.com/Sage-Bionetworks/schematic_db/blob/main/schematic_db/schema_graph/schema_graph.py)  class is used to interact with the Schematic API to create the schema in graph form, and is used by several other classes.


