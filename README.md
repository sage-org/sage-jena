# sage-jena
SaGe client made using Jena

# Getting started

First, clone and compile the project
```
git clone https://github.com/Callidon/sage-jena.git
cd sage-jena
gradle distZip
```
Then, you will find the release in `build/distributions` as a zip archive.
Unzip it, and use `bin/sage-jena` to evaluate SPARQL queries using a Sage server

# Usage

```
usage: bin/sage-jena
 -f,--file <file>            File containing a SPARQL query to execute
    --format <format>        Results format (Result set: text, XML, JSON,
                             CSV, TSV; Graph: RDF serialization)
 -h,--help                   Show help
 -q,--query <sparql-query>   SPARQL query to execute (passed in
                             command-line)
 -time                       Time the query execution
 -u,--url <url>              URL of the SaGe server
```
