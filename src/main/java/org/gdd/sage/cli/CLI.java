package org.gdd.sage.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.gdd.sage.engine.SageStageGenerator;
import org.gdd.sage.federated.factory.FederatedQueryFactory;
import org.gdd.sage.federated.factory.ServiceFederatedQueryFactory;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
                Query query = QueryFactory.create(queryString);
                FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
                factory.buildFederation();
                query = factory.getLocalizedQuery();
                Dataset federation = factory.getFederationDataset();
                // Plug-in the custom ARQ engine for Sage graphs
                StageBuilder.setGenerator(ARQ.getContext(), SageStageGenerator.createDefault());
                // Evaluate SPARQL query
                QueryExecutor executor;
                if (query.isSelectType()) {
                    executor = new SelectQueryExecutor(format);
                } else if (query.isAskType()) {
                    executor = new AskQueryExecutor();
                } else if (query.isConstructType()) {
                    executor = new ConstructQueryExecutor(format);
                } else {
                    executor = new DescribeQueryExecutor(format);
                }
                executor.execute(federation, query);
                federation.close();
            }
        } catch (ParseException e) {
            logger.error(e.getMessage());
        }
    }
}
