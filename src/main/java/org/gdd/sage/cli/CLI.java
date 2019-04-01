package org.gdd.sage.cli;

import com.google.common.collect.Lists;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.gdd.sage.core.factory.SageConfigurationFactory;
import org.gdd.sage.core.factory.SageFederatedConfiguration;
import org.gdd.sage.engine.update.UpdateExecutor;
import org.gdd.sage.http.ExecutionStats;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Main class for the Sage command-line interface
 * @author Thomas Minier
 */
@CommandLine.Command(description = "Execute a SPARQL query with the SaGe Smart client",
        name = "sage-jena", mixinStandardHelpOptions = true, version = "1.1")
public class CLI implements Callable<Void> {

    @CommandLine.Parameters(arity = "1..*", paramLabel = "URL", description = "URL(s) of SaGe server(s) to query. If several URls are provided, the query will be executed as a Federated query.")
    public String[] urls;

    @CommandLine.Option(names = { "-q", "--query" }, description = "SPARQL query to execute (passed in command-line)")
    public String query = null;

    @CommandLine.Option(names = { "-f", "--file" }, description = "File containing a SPARQL query to execute")
    public String file = null;

    @CommandLine.Option(names = { "--format" }, description = "Results format (Result set: raw, XML, JSON, CSV, TSV; Graph: RDF serialization)")
    public String format = "xml";

    @CommandLine.Option(names = { "-m", "--measure" }, description = "Measure query execution stats and append it to a file")
    public String measure = null;

    @CommandLine.Option(names = { "--update" }, description = "Execute the input query as a SPARQL UPDATE query")
    public boolean update = false;

    @CommandLine.Option(names = { "--bucket-size" }, description = "Bucket size for SPARQL UPDATE query evaluation")
    public int bucketSize = 100;

    @CommandLine.Option(names = { "--time" }, description = "Display the the query execution time at the end")
    public boolean time = false;

    @Override
    public Void call() throws Exception {
        Logger logger = ARQ.getExecLogger();
        if (this.query == null && this.file == null) {
            System.err.println("Missing required options.\n" +
                    "Parameters --query or --file are required.\n" +
                    "See sage-jena --help for more informations");
            System.exit(1);
        }
        List<String> servers = Lists.newArrayList(this.urls);
        String queryString;
        if (this.file != null) {
            queryString = "";
            try (BufferedReader r = Files.newBufferedReader(Paths.get(this.file))) {
                Optional<String> fileContent = r.lines().reduce(String::concat);
                if (fileContent.isPresent()) {
                    queryString = fileContent.get();
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
        } else {
            queryString = this.query;
        }

        Dataset federation;
        SageConfigurationFactory factory;
        ExecutionStats spy = new ExecutionStats();

        // check if we are dealing with a classic query or an UPDATE query
        if (this.update) {
            // init execution env.
            factory = new SageAutoConfiguration(servers.get(0), QueryFactory.create("SELECT * WHERE { ?s ?p ?o}"), spy);
            factory.configure();
            factory.buildDataset();
            federation = factory.getDataset();
            // execute query
            UpdateExecutor executor = new UpdateExecutor(servers.get(0), federation, bucketSize);
            spy.startTimer();
            executor.execute(queryString);
            spy.stopTimer();
        } else {
            Query parseQuery = QueryFactory.create(queryString);
            // get the auto-configuration factory based on query execution context (federated or not)
            if (servers.size() > 1) {
                factory = new SageFederatedConfiguration(servers, parseQuery, spy);
            } else {
                factory = new SageAutoConfiguration(servers.get(0), parseQuery, spy);
            }

            // Init Sage dataset (maybe federated)
            factory.configure();
            factory.buildDataset();
            parseQuery = factory.getQuery();
            federation = factory.getDataset();

            // Evaluate SPARQL query
            QueryExecutor executor;

            if (parseQuery.isSelectType()) {
                executor = new SelectQueryExecutor(format);
            } else if (parseQuery.isAskType()) {
                executor = new AskQueryExecutor(format);
            } else if (parseQuery.isConstructType()) {
                executor = new ConstructQueryExecutor(format);
            } else {
                executor = new DescribeQueryExecutor(format);
            }
            spy.startTimer();
            executor.execute(federation, parseQuery);
            spy.stopTimer();
        }

        // display execution time (if needed)
        if (this.time) {
            double duration = spy.getExecutionTime();
            int nbQueries = spy.getNbCalls();
            System.err.println(MessageFormat.format("SPARQL query executed in {0}s with {1} HTTP requests", duration, nbQueries));
        }

        // display stats in CSV format (if needed)
        // format: duration,nb HTTP requests,Avg. HTTP response time, Avg. Resume time, Avg. Suspend time
        if (this.measure != null) {
            double duration = spy.getExecutionTime();
            int nbQueries = spy.getNbCalls();
            double avgResume = spy.getMeanResumeTime();
            double avgSuspend = spy.getMeanSuspendTime();
            String csvLine = String.format("%s,%s,%s,%s,%s", duration, nbQueries, spy.getMeanHttpTimes(), avgResume, avgSuspend);
            try {
                Files.write(Paths.get(this.measure), csvLine.getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // cleanup connections
        federation.close();
        factory.close();
        return null;
    }

    public static void main(String[] args) {
        CommandLine.call(new CLI(), args);
    }
}
