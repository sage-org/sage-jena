package org.gdd.sage.cli;

import org.apache.commons.cli.*;

/**
 * CLI options sued by the Sage CLI.
 * @author Thomas Minier
 */
public class SageCLIOptions {

    private Options options;

    public SageCLIOptions() {
        Option urlOpt = Option.builder("u")
                .longOpt("url")
                .argName("url")
                .hasArg()
                .desc("URL of the SaGe server")
                .build();

        Option queryOpt = Option.builder("q")
                .longOpt("query")
                .argName("sparql-query")
                .hasArg()
                .desc("SPARQL query to execute (passed in command-line)")
                .build();

        Option fileOpt = Option.builder("f")
                .longOpt("file")
                .argName("file")
                .hasArg()
                .desc("File containing a SPARQL query to execute")
                .build();

        Option formatOpt = Option.builder()
                .longOpt("format")
                .argName("format")
                .hasArg()
                .desc("Results format (Result set: raw, XML, JSON, CSV, TSV; Graph: RDF serialization)")
                .build();

        Option measureOpt = Option.builder("m")
                .longOpt("measure")
                .argName("measure")
                .hasArg()
                .desc("Measure query execution stats and append it to a file")
                .build();

        // register options
        options = new Options();
        options.addOption(urlOpt);
        options.addOption(queryOpt);
        options.addOption(fileOpt);
        options.addOption(formatOpt);
        options.addOption(measureOpt);
        // boolean options
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
