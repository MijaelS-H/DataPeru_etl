# DataPerú ETL

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/observatory-economic-complexity/oec-etl
cd oec-etl
```

### 2. Set up a local Python environment

There are many ways to do this, but here's one:

```bash
sudo apt-get install python3-pip
sudo pip3 install virtualenv

virtualenv -p python3.7 venv
source venv/bin/activate
```

### 3. Install the project dependencies

```bash
pip install -r requirements.txt
```

### 4. Add environment variables

Refer to [this wiki](https://github.com/Datawheel/company/wiki/OEC-Environment-Variables#etl) for environment variables.

### 5. Run a pipeline to test your setup

The countries dimension pipeline is super fast to run and a great way to test that your setup works.

```bash
python etl/dim_countries_pipeline.py
```

## Data Quality Assurance

After making changes to a fact table, take the time to validate that there are no issues with the new data added.

The simplest way to start is by running the Tesseract diagnosis on the cube that has been changed like [this](https://api.oec.world/tesseract/diagnosis.jsonrecords?cube=trade_n_phl_m_hs). If you get the "Success" message, you are all done. Otherwise, check the list of errors and work on fixing them. These will often involve changes to either the fact table itself or the dimension tables that are used by this cube. Here's an example of an error:

```json
"error": [
    {
        "type":"MissingDimensionIDs",
        "message":"The following IDs for [Transport Mode].[Transport Mode].[Transport Mode] are not present in its dimension table: 0, 3."
    },
    {
        "type":"MissingDimensionIDs",
        "message":"The following IDs for [Subnat Geography].[Subnat Geography].[Subnat Geography] are not present in its dimension table: 5106, 5108, 5505, 5507."
    }
]
```

Next, take the time to perform a few queries using the explorer to make sure that the new numbers look good. For instance, one common query for cubes that have been updated with new data is to check that the totals for the new time period look roughly similar to the previous time periods. Any large difference here should lead to a closer inspection of the ingestion process. Other queries that are helpful:

- Comparing the breakdown of top products with what is live on the OEC or other sources. For example, if you're working on a subnational cube you can compare its top exports with the top exports listed on the OEC from the BACI dataset.
- Similarly, compare the top trading partners.
- Comparing the export and import totals for subnational cubes with the totals from BACI. Note that you might need to perform a currency conversion for this comparison as some of the subnational cubes are not in USD.
- Making sure that the ratio of exports to imports to a given country makes sense.

## General Comments

- Recent subnational data is often provisional and may change. For this reason, for countries whose data is easy to download, it's a good idea to download not just the latest month but also a few months before then to ensure we have more accurate data.
- For finding new subnational data to download, a good Google search query is "brazil foreign trade statistics", for example. Make sure to explore a few different sources before selecting one.
- Take a look at the README file for each subnational country for download instructions or important tips and notes.
- For ease of dealing with the large number of tables in the OEC, ALWAYS follow the naming conventions across tables. For example, in the fact table, always name the trade flow column `trade_flow_id` for consistency across tables. Check other pipelines if you're unsure how to name a column. Chances are we have included that type of data before. This makes it easier to manage all the tables and know what's in them.
- Before removing a table from the database, make sure that the table is not used anywhere in the `schema.xml` file.

## Naming Convention for Tables

When adding a new pipeline script, please use the following naming convention (for more examples, look at the existing tables in the database):

### Fact tables

*Format*: `<type>_<depth>_<identifier>_<frequency>_<classification>`

*Params*:

`type`: What the fact table represents (trade, tariffs, services, etc.).
`depth`: `i` for international, `s` subnational data, `n` for national.
`identifier`: For subnational data, this should be the `iso3` for the reporter country. For international data, this should be the organization reporting the data.
`depth`: `a` for annual, `m` for monthly, `q` for quarterly, `d` for daily.
`classification`: The classification used by this table.

*Examples*:

`trade_s_bra_a_hs` for annual Brazilian subnational trade data using the HS classification

`trade_i_comtrade_m_hs` for monthly international Comtrade trade data using the HS classification

### Dimension tables

*Format*: `dim_<identifier>_<dimension>`

*Params*:

`identifier`: For subnational data, this should be the `iso3` id for the reporter country. For international data, this should say `shared`.

`dimension`: The name of the dimension held in this table.

*Examples*:

`dim_shared_countries` for a shared countries table

`dim_rus_regions` for a Russia dimension table representing national regions

## Frequently Asked Questions

- [How do I deal with non-standard file encodings?](#how-do-i-deal-with-non-standard-file-encodings)
- [How do I import a helper function from a higher level library?](#how-do-i-import-a-helper-function-from-a-higher-level-library)
- [How to name columns in table?](#how-to-name-columns-in-table)
- [How to format country ID?](#how-to-format-country-id)
- [How do I access a specific sheet of an excel file via bamboo?](#how-do-i-access-a-specific-sheet-of-an-excel-file-via-bamboo)
- [How do I convert my product codes?](#how-do-i-convert-my-product-codes)

### How do I deal with non-standard file encodings?

Try using a util to detect encoding and then pass to pandas `read_csv()`

```python
df = pd.read_csv("Germany.csv", sep=";", encoding="latin-1", skiprows=1)
```

Reference: https://stackoverflow.com/questions/30462807/encoding-error-in-panda-read-csv

### How do I import a helper function from a higher level library?

If you followed the setup above for a local Python virtual environment, all you need to is add an extra environment variable:

```bash
export PYTHONPATH="/path/to/oec-etl"
```

### How to name columns in table?

Lower case with underscores.

### How to format country ID?

3 letter codes and lowercase, along with the 2 letter continent code. Refer to the `oec_id` column in [this spreadsheet](https://docs.google.com/spreadsheets/d/12ZL5CXZQRjtetiAg3vkcz1bU6Kjz3xJ1Z3eUDVfBc_c/edit#gid=845551346).

### How do I access a specific sheet of an excel file via Pandas?

```python
pd.read_excel("filename.xls", sheetname="sheet_name")
```

### How do I convert my product codes?

For converting 6 digit HS id to OEC ID, check out the `hs6_converter` function in the `util.py` file.
