# Source configurations #

Graphen_J allows the reading of different sources compatibly with Apache Spark.
In this section it will be explained how to configure the sources and their reading.

## How To define a source

You can define a source using two types of configuration: Format or SparkTable depending on whether you want to read a
format (e.g. jdbc, files, ...) or a table (e.g. Apache Iceberg Table, ...).

### Source Format Type

To define a Source of the Format type you have to specify the following configurations:

- `id`: this is the id of the source
- `sourceDetail`: this is an object as follows:
    - `format`: the format you want to read (e.g. csv, parquet, jdbc, ...)
    - `schema`: this is an optional configuration. You have to insert a String that represents the schema of the data in
      the
      source
    - `options`: the Apache Spark readings options (e.g. path, password, ...)

#### Example

```json
{
  "id": "source",
  "sourceDetail": {
    "format": "csv",
    "options": {
      "path": "source_path",
      "separator": ";"
    }
  }
}

```

### Source SparkTable Type

To define a Source of the SparkTable type you have to specify the following configurations:

- `id`: this is the id of the source
- `sourceDetail`: this is an object as follows:
    - `catalogName`: the name of the catalog
    - `dbName`: the name of the database
    - `tableName`: the name of the table
    - `provider`: this is an optional parameter and represents the provider of the table. For now available provider
      is `iceberg`

#### Example

```json
{
  "id": "source",
  "sourceDetail": {
    "catalogName": "source_dq_catalog",
    "dbName": "source_db",
    "tableName": "source_table",
    "provider": "iceberg"
  }
}
```

## How To configure the bookmarks

If you want to read a source using the bookmarking functionality, you can define a `bookmarkConf` field in the
configurations structured as follows:

- `bookmarkDetail`: this configuration is used to define the bookmark:
    - `bookmarkEntry`: entry with which we will refer to this source (it can be the name of the table for example).
      Important: This name must be unique for each table where bookmarks are saved.
    - `bookmarkField`: name of the field whose value will be used as bookmark
    - `bookmarkFieldType`: type of the value contained in _bookmarkField_. Supported types are `Date`, `Int`, `Long` (it
      is case insensitive)
    - `bookmarkFieldFormat`:this field is required and will be used by Graphen_J only if bookmarkFieldType will have Date
      value (e.g. `yyyy-MM-dd`)
- `bookmarkStorage`: this configuration is used to define the storage in which to save and from which to retrieve the
  bookmarks:
    - `id`: this is the id of the bookmark storage
    - `storageType`: this configuration is used to define the type of storage where bookmarks will be saved. Supported
      values are:
      - `FS`: filesystem
      - `OS`: object storage
      - `DB`: database
    - `detail`: this configuration is used to define the connection to the storage and the options for saving bookmarks.
      Depending on the value of the _storageType_ field it will have a different structure. The structure of this field
      based on type will be explained later.

### Detail field structure based on the storageType value

Depending on the value assumed by the _storageType_ field, it will be necessary to specify different details to interact
and connect to a storage.

#### OS | FS

the detail configuration, in the case of _storageType_ equal to `OS` or `FS`, must have the following structure:

- `catalogName`: the name of the catalog
- `dbName`: the name of the db
- `tableName`: the name of the table
- `provider`: this is an optional parameter and represents the provider of the table. For now available provider
  is `iceberg`
- `tableProperties`: this field represents a map of properties related to the data saving format (e.g. options supported
  by Apache Iceberg for writing)
- `options`: this field represents a map of options related to the data saving format (e.g. options supported by Apache
  Iceberg for writing)

#### DB

the detail configuration, in the case of _storageType_ equal to `DB` must have the following structure:

- `host`: the host of the database
- `user`: the username to authenticate
- `password`: the password to authenticate
- `dbName`: the database name
- `tableName`: the table name
- `dbType`: the type of the database. Supported values are:
  - `MySQL`: for the MySQL database
  - `PostgreSQL`: for the PostgreSQL database