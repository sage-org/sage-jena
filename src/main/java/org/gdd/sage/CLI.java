package org.gdd.sage;

import org.apache.commons.cli.*;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.gdd.sage.engine.SageStageGenerator;
import org.gdd.sage.model.SageModelFactory;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

public class CLI {

    public static void main(String[] args) {
        Logger logger = ARQ.getExecLogger();
        // Create options with arguments
        OptionBuilder.withLongOpt("url");
        OptionBuilder.withArgName("url");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("URL of the SaGe server");
        Option urlOpt = OptionBuilder.create("u");

        OptionBuilder.withLongOpt("query");
        OptionBuilder.withArgName("sparql-query");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("SPARQL query to execute (passed in command-line)");
        Option queryOpt = OptionBuilder.create("q");

        OptionBuilder.withLongOpt("file");
        OptionBuilder.withArgName("file");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("File containing a SPARQL query to execute");
        Option fileOpt = OptionBuilder.create("f");

        OptionBuilder.withLongOpt("format");
        OptionBuilder.withArgName("format");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Results format (Result set: text, XML, JSON, CSV, TSV; Graph: RDF serialization)");
        Option formatOpt = OptionBuilder.create();

        // register options
        Options options = new Options();
        options.addOption(urlOpt);
        options.addOption(queryOpt);
        options.addOption(fileOpt);
        options.addOption(formatOpt);
        // boolean options
        options.addOption("time", false, "Time the query execution");
        options.addOption("h", "help", false, "Show help");

        // first, check for help flags
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "sage-query", options );
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
                // Init SageModel and plug-in custom ARQ engine
                Model model = SageModelFactory.createModel(url);
                StageBuilder.setGenerator(ARQ.getContext(), SageStageGenerator.createDefault());
                // Evaluate SPARQL query
                Query query = QueryFactory.create(queryString);
                try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
                    ResultSet results;
                    if (query.isSelectType()) {
                        results = qexec.execSelect();
                        results = ResultSetFactory.copyResults(results);
                        switch (format) {
                            case "xml":
                                ResultSetFormatter.outputAsXML(results);
                                break;
                            case "json":
                                ResultSetFormatter.outputAsJSON(results);
                                break;
                            case "csv":
                                ResultSetFormatter.outputAsCSV(results);
                                break;
                            case "tsv":
                                ResultSetFormatter.outputAsTSV(results);
                                break;
                            default:
                                ResultSetFormatter.outputAsSSE(results);
                                break;
                        }
                    } else if (query.isAskType()) {
                        ResultSetFormatter.out(qexec.execAsk());
                    } else {
                        Model resultsModel = null;
                        RDFFormat modelFormat;
                        if (query.isConstructType()) {
                            resultsModel = qexec.execConstruct();
                        } else if (query.isDescribeType()) {
                            resultsModel = qexec.execDescribe();
                        } else {
                            logger.error("Unknown SPARQL query type");
                            System.exit(1);
                        }
                        switch (format) {
                            case "ttl":
                            case "turtle":
                            case "n3":
                                modelFormat = RDFFormat.TURTLE;
                                break;
                            case "nt":
                            case "n-triple":
                            case "n-triples":
                                modelFormat = RDFFormat.NTRIPLES_UTF8;
                                break;
                            case "json":
                            case "rdf/json":
                                modelFormat = RDFFormat.RDFJSON;
                                break;
                            case "jsonld":
                                modelFormat = RDFFormat.JSONLD;
                                break;
                            case "thrift":
                            case "rdf/binary":
                                modelFormat = RDFFormat.RDF_THRIFT;
                                break;
                            default:
                                modelFormat = RDFFormat.RDFXML;
                                break;
                        }
                        RDFDataMgr.write(System.out, resultsModel, modelFormat);
                    }
                }
            }
        } catch (ParseException e) {
            logger.error(e.getMessage());
        }
    }
}
