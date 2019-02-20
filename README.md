# sage-jena
SaGe client made using Jena

# Requirements

* [git](https://git-scm.com/)
* [Java 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher
* [Gradle 4.7](https://gradle.org) or higher

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

## Example usage

`bin/sage-jena http://sage.univ-nantes.fr/sparql/dbpedia-2016-04 -q "SELECT * WHERE { ?s ?p ?o } LIMIT 100"`

## Reference

```
usage: bin/sage-jena <graph-url> [options]
 -f,--file <file>            File containing a SPARQL query to execute
    --format <format>        Results format (Result set: text, XML, JSON,
                             CSV, TSV; Graph: RDF serialization)
 -h,--help                   Show help
 -q,--query <sparql-query>   SPARQL query to execute (passed in
                             command-line)
 -time                       Time the query execution
```


