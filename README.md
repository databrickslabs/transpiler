# Splunk Query Language to PySpark transpiler

![.](spark-spl.png)

## Commands

| Command/Feature | Parser | Catalyst | Codegen |
| ---: | :---: | :---: | :---: |
| search | ✅ | ✅ | ✅ |
| eval | ✅ | ✅ | ✅ |
| table | ✅ | ✅ | ✅ |
| rex |  |  |  |
| regex |  |  |  |
| where | ✅ | ✅ | ✅ |
| lookup | ✅ |  |  |
| bin |  |  |  |
| rename |  |  |  |
| join |  |  |  |
| fields |  |  |  |
| convert | ✅ |  |  |
| collect | ✅ | ✅ | ✅ |
| stats | ✅ |  |  |
| sort |  |  |  |
| return |  |  |  |
| mvcombine |  |  |  |
| map |  |  |  |
| inputlookup |  |  |  |
| head |  |  |  |
| format |  |  |  |
| fillnull |  |  |  |
| eventstats |  |  |  |
| dedup |  |  |  |
 
## Functions

| Function | Catalyst |
| ---: | :---: |
| term() | extension |
| CIDR search |  |  |  |
| strftime() |  |
| values() |  |
| latest() |  |
| earliest() |  |
| if() |  |
| mvcount() |  |
| coalesce() |  |
| mvindex() |  |
| mvappend() |  |
| null() |  |
| min() |  |
| round() |  |
| max() |  |
| substr() |  |
| isnotnull() |  |
| sum() |  |
| mvfilter() |  |
| len() |  |
| count() |  |

## Developing 

Parsers are implemented using [fastparse](https://github.com/com-lihaoyi/fastparse) (MIT)