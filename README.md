# givemedata
A convenient way to use and explore multiple databases from Python

### What is this?
Using multiple data sources may become a pain to manage. You have different credentials here and there, different db names, different drivers and wrappers. You don't like that, do you?

- Givemedata is a Python tool that allows to define a single configuration file containing all the data sources that you are using
- Then it provides a simple way to access all of those data sources leveraging the power of Pandas

Here is a simple example:

#### Step 1 (Installation):

`pip install givemedata`

#### Step 2 (Configuration):

Create a configuration file in YAML format - either in your home directory to use just by yourself or in `/etc/givemedata/`
to provide the configuration to multiple users at once (e.g. if you're using JupyterHub)

The file must be named either `givemedata.yaml` or `.givemedata.yaml` (in case if you prefer to have a hidden file)

Example of configuration:

```
Work:
  Prod:
    WebApp:
      RO: postgresql://read_only_user:secret@112.0.3.56:5432/web
      RW: postgresql://read_write_user:secret@174.0.33.1:5432/web
    Analytics: postgresql://analytics_prod_user:secret@171.0.3.34:5432/analytics
  Stage:
    Analytics: postgresql://analytics_stage_user:secret@172.0.3.67:5432/analytics
Personal:
  ML_class: postgresql://me:secret@127.0.0.1:5432/ml
```

As you can see `givemedata` supports hierarhical structure where you can logically place your data sources in a way that
is most intuitive and clear.

Basically you define a dictionary-like stucture that can be recursively nested.
Data sources are defined as connection strinsg like this: `<DB_TYPE>://<DB_USER>:<DB_PASSWORD>@<IP>:<PORT>/web_app_backend`

#### Step 3 (Loading givemedata object):

In a Python console of your choice (IPython, Jupyter, an IDE, whatever..):

`from givemedata import Data`

Data is an interface object created by `givemedata` based on the config you've just specified.
It has the same nested structure as the config and even *supports autocompletion* :)

Let's assume we want to address the Analytics db on production. This can me made as easy as this:

`db = Data.Work.Prod.Analytics`

#### Step 4 (Using data sources):

The DB object in `givemedata` have a few methods attached to it:

- `sql(query, *, limit)` -> returns an SQL query result as a Pandas DataFrame
- `public_tables` (property) -> returns a dataframe with information about all public tables in the DB.
- `all_tables` (property) -> returns a dataframe with information about all tables in the DB - both public and service.
- `public_fields` (property) -> all public fields
- `all_fields` (property) -> all fields, public and service

##### These properties also provide some helpers:

- (for both - fields and tables)
- `search(term_as_string)` -> searches the table/field names by the given term -> returns object similar to itself so can be chained
- `sample(df_index_as_int)` -> displays sample of table rows or field values

- (for tables only)
- `describe(df_index_as_int)` -> displays meta data about the table - fields, datatypes, etc

#### Limitations & Other Notes

- By now only PostgreSQL is supported as the project is in the earliest possible development stage. This was made to support my own workflow but I have plans to improve this and add support for different databases such as MySQL, SQLite3, Cassandra, etc
- Only read operations are supported (this is the first priority in the road map, so it will be fixed soon)
