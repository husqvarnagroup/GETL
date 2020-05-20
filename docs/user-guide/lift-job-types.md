# Lift job types

## load

---

### load::batch_parquet

Loads parquet data from a path.

#### Properties

- Path: path to the data

#### Example

```yml
LiftJob:
  SectionName:
    Type: load::batch_parquet
    Properties:
      Path: s3://bucket/directory/with/data
```


---

### load::batch_json

Loads json data from a path.

#### Properties

- Path: path to the data
- Suffix: suffix of the file, default .json
- JsonSchemaPath: path to the json schema
- multiline: specify if the files are multiline, default true

#### Example

```yml
LiftJob:
  SectionName:
    Type: load::batch_json
    Properties:
      Path: s3://bucket/directory/with/data
```


---


### load::batch_xml

Loads xml data from a path.

#### Properties

- Path: path to the data
- RowTag: todo
- Suffix: file suffix, default .xml
- JsonSchemaPath: path to the json schema

#### Example

```yml
LiftJob:
  SectionName:
    Type: load::batch_xml
    Properties:
      Path: s3://bucket/directory/with/data
      RowTag: row-tag-name
      Suffix: .xml
```


---


### load::batch_delta

Loads delta data from a path.

#### Example

```yml
LiftJob:
  SectionName:
    Type: load::batch_delta
    Properties:
      Path: s3://bucket/directory/with/data
```


---


### load::stream_json

Load json data as a stream.

#### Properties

- Path: path to the data
- SchemaPath: schema path

#### Example

```yml
LiftJob:
  SectionName:
    Type: load::stream_json
    Properties:
      Path: s3://bucket/directory/with/data
      SchemaPath: s3://bucket/json/schema/path
```


---


### load::jdbc

Loads jdbc data.

#### Properties

- Driver: the JDBC driver
- ConnUrl: the database url
- Table: database table
- User: database user
- Password: database password
- Query: the query

#### Example

```yml
LiftJob:
  SectionName:
    Type: load::jdbc
    Properties:
        Driver: 'org.sqlite.JDBC'
        ConnUrl: ${DBUrl}
        Table: ${DBTable}
        User: ${DBUser}
        Password: ${DBPassword}
        Alias: settings
        Query: >-
            SELECT * FROM TABLE
            WHERE name == me
```
