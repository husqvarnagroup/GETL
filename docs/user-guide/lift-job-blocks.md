# Lift job blocks

The lift job is defined by one or more lift blocks. The types of supported blocks are the following:

* [custom](#custom) - Custom codeblocks that can contain any kind of spark code
* [load](#load) - Blocks that can load data such as xml, json, delta etc.
* [transform](#transform) - Transform data with functions as alias, where and concat
* [write](#write) - Write data down as delta or to databases

This is true for each block:

* has a sub block that is annotated as `block::sub-block`
* outputs one dataframe
* needs a input dataframe (exception is the load block)

```yaml
LiftJob:
  {BlockName}
    Type: {block::sub-block}
    Input: {BlockInput}
    Properties:
      {Prop}: {Prop}

```

Example:

```yml
LiftJob:

  RawFiles:
    Type: load::batch_parquet
    Properties:
      Path: s3://bucket/path/to/data

  TransformBlock:
    Type: transform::generic
    Input: PerformOperation
    Properties:
      Functions:
        - where:
            predicate: [date, '>=', '2020-01-01']
        - select:
          - { col: name, alias: firstName, cast: string }
          - { col: age, cast: integer }
          - { col: gender }
```
  
<lift-blocks>
