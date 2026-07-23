# TransG

**TransG** is a transport greenhouse-gas (GHG) analysis tool originally developed for country-level transportation emissions work. The codebase ingests transport energy/emissions data, stores it in a local SQLite database, computes fuel use and GHG emissions, and exports country and regional summaries to Excel charts and tables.

The repository contains two main execution paths:

- a **desktop / batch workflow** for data processing and Excel output
- a **Flask web workflow** for a simple baseline comparison page

## What the tool does

At a high level, the tool:

1. reads transport emissions / fuel data from XML or CSV sources
2. loads the data into a SQLite database
3. applies emission-rate tables and fuel conversion logic
4. calculates country-level and mode-level transport GHG emissions
5. produces Excel workbooks and charts for reporting
6. exposes a small web endpoint for baseline comparisons

## Repository contents

- `Trans_GHG_Tool_Desktop.py` – main desktop/batch analysis class
- `Trans_GHG_Tool_Web.py` – web-enabled variant of the analysis class
- `WebQry_Wrapper.py` – Flask blueprint and helper for web output
- `views.py` – simple Flask route for the baseline page
- `xmlProcess.py` – parses SDMX-style generic XML into a pandas DataFrame
- `dataBase_upload.py` – creates and populates the SQLite database
- `openpyxl_Output.py` – Excel output helper using `openpyxl`
- `xlsOutput.py` – Excel output helper using `xlsxwriter`
- `LICENSE` – project license
- `.gitignore` – ignore rules

## Data flow

The typical workflow is:

1. **Parse XML input** with `xmlProcess.py`
2. **Load the parsed data** into SQLite using `dataBase_upload.py`
3. **Prepare emission-rate tables**
4. **Calculate fuel consumption and emissions** with the `TransGHG` class
5. **Aggregate by country, mode, and year**
6. **Export results** to Excel or display them through Flask

## Main functionality

### XML parsing

`xmlProcess.py` expects SDMX-style generic XML with observations keyed by:

- `COUNTRY`
- `FLOW`
- `PRODUCT`
- `TIME`

It converts the XML into a flat table with columns like:

- `COUNTRY`
- `FLOW`
- `PRODUCT`
- `TIME`
- `OBS`
- `OBS_STATUS`

### Database preparation

`dataBase_upload.py` creates a SQLite-backed workflow and expects tables such as:

- `FuelAll`
- `EmisRates`
- `EmisRatesFinal`
- `CountryLkup`
- `ProductLkup`
- `FlowLkup`

It also includes preprocessing logic to establish primary keys and standardize imported data.

### Emissions calculations

The `TransGHG` class in `Trans_GHG_Tool_Desktop.py` and `Trans_GHG_Tool_Web.py` supports:

- calculating fuel quantities from CO2 observations
- generating adjusted emission-rate tables
- calculating CO2, CH4, N2O, and total GHG emissions
- aggregating results by country, mode, product, and year

### Output generation

The project can create Excel outputs using either:

- `openpyxl`
- `xlsxwriter`

These outputs include:

- detailed tables
- country comparisons
- time series charts
- mode contribution charts

### Web interface

`WebQry_Wrapper.py` and `views.py` provide a lightweight Flask route that returns baseline comparison data as JSON or renders a child template.

## Requirements

The code was written for Python and uses the following libraries:

- `pandas`
- `sqlalchemy`
- `sqlite3` (built in)
- `xml.etree.ElementTree` (built in)
- `flask`
- `openpyxl`
- `xlsxwriter`

Depending on how you run it, you may also need:

- `pymysql` or `mariadb` connectors if you switch away from SQLite
- the `scripts_bank` package referenced in `Trans_GHG_Tool_Web.py`

## Suggested environment setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas sqlalchemy flask openpyxl xlsxwriter
```

If you plan to use the web code or a non-SQLite database, install the relevant extra packages as well.

## Important configuration note

The original code contains **hardcoded local Windows paths** and some project-specific imports.
Before running it in a new environment, you will likely need to update:

- the SQLite connection path in `dataBase_upload.py`
- any local file paths used for input XML or output Excel files
- package imports if this project is not installed under the same folder structure as the original environment

## Example usage

The code is class-based rather than a single CLI application. A typical workflow would look like:

```python
from Trans_GHG_Tool_Desktop import TransGHG
from dataBase_upload import dbSetup
from xmlProcess import xmlProcess
from openpyxl_Output import openpyxl_Output

# 1. Parse the XML input
parser = xmlProcess("path/to/DataGeneric.xml")
df = parser.convDf()

# 2. Load into SQLite
loader = dbSetup("Transport.db")
loader.readData("SQLite", df, "FuelAll")
loader.Preprocess("SQLite")

# 3. Run calculations
model = TransGHG("SQLite")
model.genERtabl("Default")
model.FuelCalc()
model.EmisCalc()

# 4. Query and export results
country_df = model.qryAggData("India", [2011, 2018], "Detailed")
```

The exact sequence depends on the available tables and your data structure.

## Expected inputs

The code expects transport-related country data that can be mapped into the database tables used by the model. In practice, this usually means:

- country identifiers
- transport flow / mode categories
- product or fuel categories
- annual time series data
- observation values and status flags

## Expected outputs

Typical outputs include:

- country-level transport GHG totals
- mode-level emissions breakdowns
- yearly time series summaries
- Excel workbooks with charts and tables
- JSON output for the web layer

## Known limitations in the current code snapshot

This repository is an early research / prototype codebase. A few things stand out:

- some functions still contain incomplete or placeholder logic
- there are hardcoded development paths
- several methods include `pdb.set_trace()` debug breakpoints
- some SQL statements appear to have formatting issues that may need cleanup before execution
- the web file references a package path (`scripts_bank.baseline`) that may not exist outside the original environment

## Suggested next step

If you want to revive this tool, the best path is to:

1. clean the file structure into a proper Python package
2. replace hardcoded paths with configuration variables
3. remove debug breakpoints
4. verify the database schema and sample data
5. add a small reproducible example dataset
6. write a command-line entry point and a short test suite

## License

See `LICENSE` for the project license.
