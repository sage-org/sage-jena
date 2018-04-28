package org.gdd.sage;

import org.apache.commons.cli.*;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.gdd.sage.engine.SageStageGenerator;
import org.gdd.sage.model.SageModelFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CLI {

    public static void main(String[] args) {
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
                System.err.println("Missing required parameters." +
                        "Parameters --url and --query of --file are required." +
                        "\nSee sage-query --help for more informations");
                System.exit(1);
            } else {
                String url = cmd.getOptionValue("url");
                String queryString;
                if (cmd.hasOption("file")) {
                    queryString = "";
                    try (BufferedReader r = Files.newBufferedReader(Paths.get(cmd.getOptionValue("file")))) {
                        queryString = r.lines().reduce(String::concat).get();
                    } catch (IOException e) {
                        e.printStackTrace();
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
                    ResultSet results = qexec.execSelect();
                    results = ResultSetFactory.copyResults(results);
                    ResultSetFormatter.out(System.out, results, query);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
