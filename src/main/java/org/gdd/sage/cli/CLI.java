package org.gdd.sage.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.gdd.sage.engine.SageExecutionContext;
import org.gdd.sage.federated.factory.FederatedQueryFactory;
import org.gdd.sage.federated.factory.ServiceFederatedQueryFactory;
import org.gdd.sage.model.SageGraph;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Optional;

public class CLI {

    private static int computeNbQueries(Dataset federation) {
        final int[] nbQueries = {0};
        nbQueries[0] += ((SageGraph) federation.getDefaultModel().getGraph()).getClient().getNbQueries();
        federation.listNames().forEachRemaining(s -> {
            nbQueries[0] += ((SageGraph) federation.getNamedModel(s).getGraph()).getClient().getNbQueries();
        });
        return nbQueries[0];
    }

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
                Query query = QueryFactory.create(queryString);
                FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
                factory.buildFederation();
                query = factory.getLocalizedQuery();
                Dataset federation = factory.getFederationDataset();
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
                long startTime = System.nanoTime();
                executor.execute(federation, query);
                long endTime = System.nanoTime();
                if (cmd.hasOption("time")) {
                    double duration = (endTime - startTime) / 10e9;
                    int nbQueries = computeNbQueries(federation);
                    System.err.println(MessageFormat.format("SPARQL query executed in {0}s with {1} HTTP requests", duration , nbQueries));
                }
                federation.close();
            }
        } catch (ParseException e) {
            logger.error(e.getMessage());
        }
    }
}
