package org.gdd.sage.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.gdd.sage.core.factory.SageConfigurationFactory;
import org.gdd.sage.core.factory.SageFederatedConfiguration;
import org.gdd.sage.engine.SageExecutionContext;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.gdd.sage.http.ExecutionStats;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;

/**
 * Main class for the Sage command-line interface
 * @author Thomas Minier
 */
public class CLI {

    public static void main(String[] args) {
        Logger logger = ARQ.getExecLogger();
        SageCLIOptions options = new SageCLIOptions();
        try {
            CommandLine cmd = options.parseArgs(args);
            if (cmd.hasOption("help")) {
                options.printHelp();
            } else if (cmd.getArgList().isEmpty()) {
                System.err.println("Missing required arguments. You must pass the URL of at least one RDF Graph to query.\n" +
                        "See sage-jena --help for more informations");
                System.exit(1);
            } else if (((!cmd.hasOption("query")) && (!cmd.hasOption("file")))) {
                System.err.println("Missing required options.\n" +
                        "Parameters --query or --file are required.\n" +
                        "See sage-jena --help for more informations");
                System.exit(1);
            } else {
                List<String> urls = cmd.getArgList();
                String queryString;
                String format = "xml";
                if (cmd.hasOption("format")) {
                    format = cmd.getOptionValue("format").toLowerCase();
                }
                if (cmd.hasOption("file")) {
                    queryString = "";
                    try (BufferedReader r = Files.newBufferedReader(Paths.get(cmd.getOptionValue("file")))) {
                        Optional<String> fileContent = r.lines().reduce(String::concat);
                        if (fileContent.isPresent()) {
                            queryString = fileContent.get();
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                        System.exit(1);
                    }
                } else {
                    queryString = cmd.getOptionValue("query");
                }

                ExecutionStats spy = new ExecutionStats();
                Query query = QueryFactory.create(queryString);

                // get the auto-configuration factory based on query execution context (federated or not)
                SageConfigurationFactory factory;
                if (urls.size() > 1) {
                    factory = new SageFederatedConfiguration(urls, query, spy);
                } else {
                    factory = new SageAutoConfiguration(urls.get(0), query, spy);
                }

                // Init Sage dataset (maybe federated)
                factory.buildDataset();
                query = factory.getQuery();
                Dataset federation = factory.getDataset();

                // Plug-in the custom ARQ engine for Sage graphs
                SageExecutionContext.configureDefault(ARQ.getContext());

                // Evaluate SPARQL query
                QueryExecutor executor;

                if (query.isSelectType()) {
                    executor = new SelectQueryExecutor(format);
                } else if (query.isAskType()) {
                    executor = new AskQueryExecutor(format);
                } else if (query.isConstructType()) {
                    executor = new ConstructQueryExecutor(format);
                } else {
                    executor = new DescribeQueryExecutor(format);
                }
                spy.startTimer();
                executor.execute(federation, query);
                spy.stopTimer();
                if (cmd.hasOption("time")) {
                    double duration = spy.getExecutionTime();
                    int nbQueries = spy.getNbCalls();
                    System.err.println(MessageFormat.format("SPARQL query executed in {0}s with {1} HTTP requests", duration , nbQueries));
                }
                if (cmd.hasOption("measure")) {
                    double duration = spy.getExecutionTime();
                    int nbQueries = spy.getNbCalls();
                    double avgImport = spy.getMeanImportTimes();
                    double avgExport = spy.getMeanExportTimes();
                    String measure = String.format("%s,%s,%s,%s,%s", duration, nbQueries, spy.getMeanHttpTimes(), avgImport, avgExport);
                    Files.write(Paths.get(cmd.getOptionValue("measure")), measure.getBytes(), StandardOpenOption.APPEND);
                }
                federation.close();
            }
        } catch (ParseException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
