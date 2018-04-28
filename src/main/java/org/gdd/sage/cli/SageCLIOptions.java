package org.gdd.sage.cli;

import org.apache.commons.cli.*;

public class SageCLIOptions {

    private Options options;

    public SageCLIOptions() {
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
        options = new Options();
        options.addOption(urlOpt);
        options.addOption(queryOpt);
        options.addOption(fileOpt);
        options.addOption(formatOpt);
        // boolean options
        options.addOption("time", false, "Time the query execution");
        options.addOption("h", "help", false, "Show help");
    }

    public CommandLine parseArgs(String[] args) throws ParseException {
        CommandLineParser parser = new BasicParser();
        return parser.parse(options, args);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "sage-query", options);
    }
}
