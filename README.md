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

```
Usage: sage-jena [-hV] [--time] [--update] [--bucket-size=<bucketSize>]
                 [--format=<format>] [-f=<file>] [-m=<measure>] [-q=<query>]
                 URL...
Execute a SPARQL query with the SaGe Smart client
      URL...                URL(s) of SaGe server(s) to query. If several URls are
                              provided, the query will be executed as a Federated
                              query.
      --bucket-size=<bucketSize>
                            Bucket size for SPARQL UPDATE query evaluation
      --format=<format>     Results format (Result set: raw, XML, JSON, CSV, TSV;
                              Graph: RDF serialization)
      --time                Display the the query execution time at the end
      --update              Execute the input query as a SPARQL UPDATE query
  -f, --file=<file>         File containing a SPARQL query to execute
  -h, --help                Show this help message and exit.
  -m, --measure=<measure>   Measure query execution stats and append it to a file
  -q, --query=<query>       SPARQL query to execute (passed in command-line)
  -V, --version             Print version information and exit.
```

## Example: simple query

The following example finds the first 100 RDf triples in the DBpedia 2016 dataset

```
bin/sage-jena http://sage.univ-nantes.fr/sparql/dbpedia-2016-04 -q "SELECT * WHERE { ?s ?p ?o } LIMIT 100"
```

## Example: federated SPARQL query

The next example shows how to execute a federated SPARQL in the FedX/Anapasid style.   
You simply have ot provide a set of RDF graphs urls and a SPARQL query, and
then **the query engine will automatically rewrite the query** by using source selection and query decomposition techniques.

```bash
# store the query in a variable (to simplify the example)
export QUERY="SELECT * WHERE { <http://dbpedia.org/resource/Albert_Einstein> <http://www.w3.org/2002/07/owl#sameAs> ?cc . ?cc <http://www.w3.org/2000/01/rdf-schema#label> ?name. }"

# execute the query
bin/sage-jena http://sage.univ-nantes.fr/sparql/dbpedia-2016-04 http://sage.univ-nantes.fr/sparql/sameAs -q "$QUERY"
```

