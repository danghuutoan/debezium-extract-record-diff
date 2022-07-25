Modification version of Debezium [ExtractNewRecordState](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)

This SMT supports extracting changed field of updated data

Example on how to add to your connector:
```
"transforms": "unwrap",
"transforms.unwrap.type": "com.github.danghuutoan.debezium.transforms.ExtractOldRecordState",
"transforms.unwrap.drop.tombstones": "false",
"transforms.unwrap.delete.handling.mode":"rewrite",
```
For an update event to contain the previous values of all columns in the row, you would have to change the customers table by running ALTER TABLE customers REPLICA IDENTITY FULL.

view current REPLICA IDENTITY
```
SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'customers'::regclass;
```