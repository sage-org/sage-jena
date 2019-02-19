package org.gdd.sage.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
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
import java.util.Optional;

public class CLI {

    public static void main(String[] args) {
        Logger logger = ARQ.getExecLogger();
        SageCLIOptions options = new SageCLIOptions();
        try {
            CommandLine cmd = options.parseArgs(args);
            if (cmd.hasOption("help")) {
                options.printHelp();
            } else if ((!cmd.hasOption("url")) && ((!cmd.hasOption("query")) || (!cmd.hasOption("file")))) {
                logger.error("Missing required parameters." +
                        "Parameters --url and --query of --file are required." +
                        "\nSee sage-query --help for more informations");
                System.exit(1);
            } else {
                String url = cmd.getOptionValue("url");
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
                // Init Sage dataset (maybe federated)
                ExecutionStats spy = new ExecutionStats();
                Query query = QueryFactory.create(queryString);
                SageAutoConfiguration factory = new SageAutoConfiguration(url, query, spy);
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
