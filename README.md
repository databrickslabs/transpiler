# Splunk Query Language to PySpark transpiler

![.](spark-spl.png)

## Compatibility

| Command/Feature | Parser | Catalyst | Codegen |
| ---: | :---: | :---: | :---: |
| search | ✅ | ✅ | ✅ |
| TERM(..) |  | ✅ |  |
| CIDR search |  |  |  |
| eval | ✅ | ✅ | ✅ |
| convert | ✅ |  |  |
| collect | ✅ | ✅ | ✅ |
| lookup | ✅ |  |  |
| where | ✅ | ✅ | ✅ |
| table | ✅ | ✅ | ✅ |
| stats | ✅ |  |  |
