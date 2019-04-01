package org.gdd.sage.cli;

import org.apache.commons.cli.*;

/**
 * CLI options used by the Sage CLI.
 * @author Thomas Minier
 */
public class SageCLIOptions {

    private Options options;

    public SageCLIOptions() {
        Option queryOpt = Option.builder("q")
                .longOpt("query")
                .argName("sparql-query")
                .hasArg(true)
                .desc("SPARQL query to execute (passed in command-line)")
                .type(String.class)
                .build();

        Option fileOpt = Option.builder("f")
                .longOpt("file")
                .argName("file")
                .hasArg(true)
                .type(String.class)
                .desc("File containing a SPARQL query to execute")
                .build();

        Option formatOpt = Option.builder()
                .longOpt("format")
                .argName("format")
                .hasArg(true)
                .type(String.class)
                .desc("Results format (Result set: raw, XML, JSON, CSV, TSV; Graph: RDF serialization)")
                .build();

        Option measureOpt = Option.builder("m")
                .longOpt("measure")
                .argName("measure")
                .hasArg(true)
                .type(String.class)
                .desc("Measure query execution stats and append it to a file")
                .build();

        Option bucketSizeOpt = Option.builder()
                .longOpt("bucket")
                .argName("update-bucket-size")
                .hasArg(true)
                .type(Number.class)
                .desc("Bucket size for SPARQL UPDATE query evaluation")
                .build();

        // register options
        options = new Options();
        options.addOption(queryOpt);
        options.addOption(fileOpt);
        options.addOption(formatOpt);
        options.addOption(measureOpt);
        options.addOption(bucketSizeOpt);
        // boolean options
        options.addOption( "update", false, "Execute the input query as a SPARQL UPDATE query");
        options.addOption("time", false, "Display the the query execution time at the end");
        options.addOption("h", "help", false, "Show help");
    }

    public CommandLine parseArgs(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "sage-query", options);
    }
}
