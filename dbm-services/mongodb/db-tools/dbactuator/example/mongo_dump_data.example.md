# MongoDB Dump Data Example

This example demonstrates how to use the `mongo_dump_data` atomic job to dump data from a MongoDB instance.

## Basic Usage

```bash
./mongo-dbactuator --uid="test_uid" --root_id="test_root" --node_id="test_node" --version_id="test_version" --atom-job-list="mongo_dump_data" --payload="<base64_encoded_json>"
```

## Parameters

The payload should be a base64-encoded JSON with the following structure:

```json
{
  "bk_dbm_instance": {
    "bk_biz_id": 123,
    "bk_cloud_id": 0,
    "cluster_domain": "test.mongodb.cluster",
    "cluster_name": "test-mongodb-cluster",
    "cluster_type": "MongoReplicaSet",
    "instance_role": "mongo_m1",
    "machine_type": "mongodb"
  },
  "ip": "127.0.0.1",
  "port": 27017,
  "adminUsername": "admin",
  "adminPassword": "password123",
  "outputPath": "/data/dbbak/mongodump-custom",
  "maxConcurrency": 4,
  "compressOutput": true,
  "args": {
    "backupNode": "primary",
    "isPartial": false,
    "oplog": true,
    "nsFilter": {
      "db_patterns": [],
      "ignore_dbs": ["admin", "config"],
      "table_patterns": [],
      "ignore_tables": []
    },
    "query": ""
  }
}
```

## Parameter Description

- `bk_dbm_instance`: DBM instance metadata
- `ip`: MongoDB instance IP address
- `port`: MongoDB instance port
- `adminUsername`: Admin username for authentication
- `adminPassword`: Admin password for authentication
- `outputPath`: Custom output path for dump files (optional)
- `maxConcurrency`: Maximum concurrency for dump operations (default: 4)
- `compressOutput`: Whether to compress output files
- `args.backupNode`: Which node to backup from (primary/secondary)
- `args.isPartial`: Whether to perform partial dump
- `args.oplog`: Whether to include oplog (only valid when isPartial is false)
- `args.nsFilter`: Namespace filter for partial dumps
- `args.query`: Query filter for documents (JSON string, works only with partial dumps and specific collections)

## Examples

### Full Database Dump with Oplog

```json
{
  "ip": "127.0.0.1",
  "port": 27017,
  "adminUsername": "admin",
  "adminPassword": "password123",
  "args": {
    "isPartial": false,
    "oplog": true
  }
}
```

### Partial Dump with Database Filter

```json
{
  "ip": "127.0.0.1",
  "port": 27017,
  "adminUsername": "admin",
  "adminPassword": "password123",
  "args": {
    "isPartial": true,
    "nsFilter": {
      "db_patterns": ["myapp*", "userdata"],
      "ignore_dbs": ["admin", "config", "local"]
    }
  }
}
```

### Partial Dump with Query Filter

```json
{
  "ip": "127.0.0.1",
  "port": 27017,
  "adminUsername": "admin",
  "adminPassword": "password123",
  "args": {
    "isPartial": true,
    "nsFilter": {
      "db_patterns": ["myapp"],
      "table_patterns": ["users"]
    },
    "query": "{\"status\": \"active\", \"created_at\": {\"$gte\": \"2023-01-01\"}}"
  }
}
```

### Advanced Query Examples

#### Date Range Query
```json
{
  "args": {
    "isPartial": true,
    "nsFilter": {
      "db_patterns": ["analytics"],
      "table_patterns": ["events"]
    },
    "query": "{\"timestamp\": {\"$gte\": ISODate(\"2023-01-01T00:00:00Z\"), \"$lt\": ISODate(\"2023-02-01T00:00:00Z\")}, \"type\": \"user_action\"}"
  }
}
```

#### Complex Query with Multiple Conditions
```json
{
  "args": {
    "isPartial": true,
    "nsFilter": {
      "db_patterns": ["ecommerce"],
      "table_patterns": ["orders"]
    },
    "query": "{\"$and\": [{\"status\": {\"$in\": [\"completed\", \"shipped\"]}}, {\"total_amount\": {\"$gte\": 100}}, {\"customer.region\": \"US\"}]}"
  }
}
```

## Notes

- The job cannot be executed as root user for security reasons
- Default output path will be generated if not specified
- Compression is optional and can improve storage efficiency
- Namespace filtering allows fine-grained control over what data to dump
- **Query filters work only with partial dumps and specific collections** - mongodump's `--query` parameter requires the `--collection` parameter
- Query strings must be valid MongoDB query documents in JSON format
- For full database dumps with query filtering, consider using partial dump mode with appropriate namespace filters
- The job supports concurrent operations to improve performance
- When using queries, ensure proper escaping of JSON strings in the configuration