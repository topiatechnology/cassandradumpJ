package com.topiatechnology.cassandradumpJ;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.datastax.driver.core.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class CassandraDumpJ {

    //Values used in the Python script, which I will also use
    private static final int TIMEOUT_SEC = 120;
    private static final int FETCH_SIZE = 100; //TODO - what am I for?
    private static final int DOT_EVERY = 1000;
    private static final int CONCURRENT_BATCH_SIZE = 1000;

    //========= ARGS =============
    int CONNECT_TIMEOUT = 5;
    String[] CF = new String[0];
    File EXPORT_FILE = null;
    String[] FILTER = new String[0];
    String HOST = "localhost";
    int PORT = 9042;
    File IMPORT_FILE = null;
    String[] KEYSPACE = new String[0];
    String[] EXCLUDE_CF = new String[0];
    boolean NO_CREATE = false;
    boolean NO_INSERT = false;
    String PASSWORD = null;
    int PROTOCOL_VERSION = -1;
    boolean QUIET = false;
    boolean SYNC = false;
    String USERNAME = null;
    int LIMIT = 0;
    boolean SSL = false;
    File CERTFILE = null;
    File USERKEY = null;
    File USERCERT = null;
    //======= END ARGS ===========

    private Cluster _cluster = null;
    private Session _session = null;

    private static final Options cli_parser = constructParserArgs();

    public static void main(String[] args) {
        CassandraDumpJ instance = parseCommandLineOptionsToInstance(args);

        try {
            instance.setupCluster();

            if(instance.IMPORT_FILE != null) {
                instance.importData();
            } else if(instance.EXPORT_FILE != null) {
                instance.exportData();
            }
        } catch (ExecutionException | TimeoutException e) {
            System.err.println("Failed to create session");
            e.printStackTrace();
            System.exit(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(3);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(4);
        } finally {
            instance.cleanupCluster();
        }
    }

    private void setupCluster() throws ExecutionException, InterruptedException, TimeoutException {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoint(HOST)
                .withPort(PORT);

        //Unimplemented. Java SSL is a bit of a nightmare.
        //If you're reading this and you're trying to do SSL, I apologize, but _this_ is a rat's nest.
        //I _suspect_ we'd need to do something with classes like the X509ExtendedTrustManager.
        //IBM has some interesting stuff about it here https://www.ibm.com/docs/en/sdk-java-technology/8?topic=interfaces-x509extendedtrustmanager-class
        if(SSL && CERTFILE != null) {
            SSLContext context = null;
            try {
                context = SSLContext.getInstance("SSL_TLS");

                KeyManager[] keyManagers = null; //TODO: figure out how to add the specified stuff to the default version of this
                TrustManager[] trustManagers = null; //TODO: figure out how to add the specified stuff to the default version of this
                context.init(keyManagers, trustManagers, new SecureRandom());
                RemoteEndpointAwareJdkSSLOptions sslOpts = RemoteEndpointAwareJdkSSLOptions.builder()
                        .withSSLContext(context)
                        .build();
                builder.withSSL(sslOpts);
                throw new NotImplementedException();
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new RuntimeException("Fatal exception attempting to activate SSL", e);
            }
        }
        if(PROTOCOL_VERSION > 0) {
            if(PROTOCOL_VERSION == 1) {
                //This is where we'd handle setting up protocol-version 1 authentication, but I have no idea how to do that.
                throw new NotImplementedException();
            } else {
                //Notably, "anything bigger than 1" activates this code, which is pretty far from future-proof, but this is how the python handled it
                builder.withAuthProvider(new PlainTextAuthProvider(USERNAME, PASSWORD));
            }
        }
        _cluster = builder.build();
        _session = _cluster.connectAsync().get(CONNECT_TIMEOUT, TimeUnit.SECONDS);
    }

    private void cleanupCluster() {
        if(_session != null) {
            _session.close();
            _session = null;
        }
        if(_cluster != null) {
            _cluster.close();
            _cluster = null;
        }
    }

    private void logQuiet(String msg) {
        if(!QUIET) {
            System.out.print(msg);
        }
    }

    private KeyspaceMetadata getKeyspaceOrFail(String keyname) {
        KeyspaceMetadata km = _cluster.getMetadata().getKeyspace(keyname);
        if(km == null) {
            System.err.println("Can't find keyspace " + keyname);
            throw new RuntimeException();
        }
        return km;
    }

    private TableMetadata getColumnFamilyOrFail(KeyspaceMetadata keyspace, String tablename) {
        TableMetadata tableval = keyspace.getTable(tablename);

        if (tableval == null) {
            System.err.println("Can't find table \"" + tablename + "\"\n");
            throw new RuntimeException();
        }

        return tableval;
    }

    private void tableToCQLFile(String keyspace, String tablename, String flt, TableMetadata tableval, FileWriter filep) throws IOException {
        String query;
        if(flt == null) {
            query = "SELECT * FROM \"" + keyspace + "\".\"" + tablename + "\"";
        } else {
            query = "SELECT * FROM " + flt;
        }
        if(LIMIT > 0) {
            query += " LIMIT " + LIMIT;
        }

        ResultSet rows = _session.execute(query);

        int cnt = 0;

        /*
        As far as I know, there is no distinctly clean way to straight port this code into Java.
        To the best of my ability, I am going to try to essentially translate it, rewriting it to maintain
        all of the original behavior, but in a Java style.

        I will try my best to explain what (I think) the Python is doing, and what I am doing to emulate that behavior
         */

        filep.write("CONSISTENCY ONE;\n");
        for (Row row : rows) {
            ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
            //In the python make_row_encoder, the code is deciding how to handle a row based on whether or not
            //It contains a column of type "counter".
            //We will scan the columns now to see if a "counter" column exists
            //
            //The defs for make_value_encoders, make_value_encoder, and make_non_null_value_encoder are tasked with
            //Coming up with the mechanism by which a datatype can be converted into a utf8 String.
            //In this code, we will be converting and pushing those values onto these Maps instead.
            //The python kept the counters separate from the non-counters, so we will do the same.
            //The standard DataStax mechanism for converting types to CQL String literals is via the
            //TypeCodec.format() function, for which there are many TypeCodecs.
            //The mechanism by which you convert to/from a DataType to/from a TypeCodec is using the CodecRegistry
            //which you can ask the cluster for, like so
            CodecRegistry codecRegistry = _cluster.getConfiguration().getCodecRegistry();
            Map<String, String> encodedColumnCounterValues = new HashMap<>();
            Map<String, String> encodedColumnNonCounterValues = new HashMap<>();
            for(ColumnDefinitions.Definition columnDefinition : columnDefinitions) {
                Object value = row.getObject(columnDefinition.getName());
                String encodedValue = codecRegistry.codecFor(columnDefinition.getType()).format(value);
                if(columnDefinition.getType().equals(DataType.counter())) {
                    encodedColumnCounterValues.put(columnDefinition.getName(), encodedValue);
                } else {
                    encodedColumnNonCounterValues.put(columnDefinition.getName(), encodedValue);
                }
            }
            String lineToAdd;
            if(!encodedColumnCounterValues.isEmpty()) {
                String setClause = encodedColumnCounterValues.entrySet()
                        .stream()
                        .map((e -> e.getKey() + " = " + e.getKey() + " + " + e.getValue()))
                        .collect(Collectors.joining(", "));
                String whereClause = encodedColumnNonCounterValues.entrySet()
                        .stream()
                        .map((e -> e.getKey() + " = " + e.getValue()))
                        .collect(Collectors.joining(" AND "));
                lineToAdd = "UPDATE \""+keyspace+"\".\""+tablename+"\" SET " + setClause + " WHERE " + whereClause;
            } else {
                String columns = encodedColumnNonCounterValues.entrySet()
                        .stream()
                        .filter(e -> e.getValue() != null)
                        .map(e -> "\"" + e.getKey() + "\"")
                        .collect(Collectors.joining(", "));
                String values = encodedColumnNonCounterValues.entrySet()
                        .stream()
                        .filter(e -> e.getValue() != null)
                        .map(e -> e.getValue())
                        .collect(Collectors.joining(", "));
                lineToAdd = "INSERT INTO \""+keyspace+"\".\""+tablename+"\" ("+columns+") VALUES ("+values+")";
            }
            filep.write(lineToAdd + ";\n");

            cnt++;

            if (cnt % DOT_EVERY == 0) {
                logQuiet(".");
            }
        }

        if (cnt > DOT_EVERY) {
            logQuiet("\n");
        }
    }

    private void exportData() throws IOException {
        int selection_options = 0;

        if (KEYSPACE.length > 0) {
            selection_options++;
        }

        if(CF.length > 0) {
            selection_options++;
        }

        if(FILTER.length > 0) {
            selection_options++;
        }

        if (selection_options > 1) {
            System.err.println("--cf, --keyspace and --filter can't be combined");
            throw new RuntimeException();
        }

        FileWriter f = new FileWriter(EXPORT_FILE);
        try {
            if (selection_options == 0) {
                logQuiet("Exporting all keyspaces\n");
            }
            List<String> keyspaces;
            if (KEYSPACE.length == 0) {
                keyspaces = _cluster.getMetadata().getKeyspaces().stream().map(KeyspaceMetadata::getName).collect(Collectors.toList());
                keyspaces.remove("system");
                keyspaces.remove("system_traces");
            } else {
                keyspaces = new ArrayList<>(Arrays.asList(KEYSPACE));
            }
            List<String> exclude_list = new ArrayList<>(Arrays.asList(EXCLUDE_CF)); //I think this was a bug in the python - --exclude-cf would only be honored if --keyspaces was specified. I'm changing the behavior so that --exclude-cf is always honored
            //limit already loaded and parsed and the default is handled

            //if keyspaces is not NONE is redundant in Java - the keyspaces variable will never be null here
            for (String keyname : keyspaces) {
                KeyspaceMetadata keyspace = getKeyspaceOrFail(keyname);

                if (!NO_CREATE) {
                    logQuiet("Exporting schema for keyspace " + keyname + "\n");
                    f.write("CONSISTENCY ALL;\n");
                    f.write("DROP KEYSPACE IF EXISTS \"" + keyname + "\";\n");
                    f.write(keyspace.exportAsString() + "\n");
                }
                for(TableMetadata tableval : keyspace.getTables()) {
                    if(exclude_list.contains(tableval.getName())) {
                        logQuiet("Skipping data export for column family " + keyname + "." + tableval.getName() + "\n");
                        continue;
                    }
                    //TODO - figure out how to ask elif tableval.is_cql_compatible in Java
                    if(!NO_INSERT) {
                        logQuiet("Exporting data for column family " + keyname + "." + tableval.getName() + "\n");
                        tableToCQLFile(keyname, tableval.getName(), null, tableval, f);
                    }
                }
            }
            //if args.cf is not None is redundant in Java - CF will never be null here
            for (String cf : CF) {
                if(!cf.contains(".")) {
                    System.err.println("Invalid keyspace.column_family input\n");
                    throw new RuntimeException();
                }
                String keyname = cf.split("\\.")[0];
                String tablename = cf.split("\\.")[1];
                KeyspaceMetadata keyspace = getKeyspaceOrFail(keyname);
                TableMetadata tableval = getColumnFamilyOrFail(keyspace, tablename);
                //TODO - figure out how to ask elif tableval.is_cql_compatible in Java
                if(!NO_CREATE) {
                    logQuiet("Exporting schema for column family " + keyname + "." + tablename + "\n");
                    f.write("DROP TABLE IF EXISTS \"" + keyname + "\".\"" + tablename + "\";\n");
                    f.write(tableval.exportAsString() + ";\n");
                }
                if(!NO_INSERT) {
                    logQuiet("Exporting data for column family " + keyname + "." + tableval.getName() + "\n");
                    tableToCQLFile(keyname, tableval.getName(), null, tableval, f);
                }
            }
            //if args.filter is not None is redundant in Java - FILTER will never be null here
            for (String flt : FILTER) {
                String stripped = flt.trim();
                String cf = stripped.split(" ")[0];
                if(!cf.contains(".")) {
                    System.err.println("Invalid keyspace.column_family input\n");
                    throw new RuntimeException();
                }
                String keyname = cf.split("\\.")[0];
                String tablename = cf.split("\\.")[1];
                KeyspaceMetadata keyspace = getKeyspaceOrFail(keyname);
                TableMetadata tableval = getColumnFamilyOrFail(keyspace, tablename);
                //The python needlessly double-checks for a null tableval here - it's already checked by getColumnFamilyOrFail
                if(!NO_INSERT) {
                    logQuiet("Exporting data for filter \"" + stripped + "\"\n");
                    tableToCQLFile(keyname, tablename, stripped, tableval, f);
                }
            }
        } finally {
            f.close();
        }
    }

    private boolean canExecuteConcurrently(String statement) {
        if(SYNC) {
            return false;
        }
        return (statement.toUpperCase().startsWith("INSERT") || statement.toUpperCase().startsWith("UPDATE"));
    }

    private void importData() {
        BufferedReader importFileReader = null;
        try {
            importFileReader = new BufferedReader(new FileReader(IMPORT_FILE));

            int cnt = 0;

            String statement = "";
            List<String> concurrentStatements = new ArrayList<>();

            ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

            String importLine = importFileReader.readLine();
            while (importLine != null) {
                statement += importLine;
                //if(statement.endsWith(";\n")) {
                if(statement.endsWith(";")) {
                    if(statement.startsWith("CONSISTENCY ")) {
                        //This is new code that wasn't in the python. It appears that we have to handle
                        //Consistency changes a bit more manually than just feeding them in.
                        String conLvl = statement.split(" ")[1].replace(";", "");
                        ConsistencyLevel newConLvl = ConsistencyLevel.valueOf(conLvl);
                        //Only update the consistencyLevel when it actually changes.
                        if(consistencyLevel != newConLvl) {
                            logQuiet("Updating CONSISTENCY to " + conLvl + "\n");
                            //We're changing consistency level, so we should execute any built-up
                            //concurrent statements at the prior level
                            for(String cStatement : concurrentStatements) {
                                _session.execute(_session.prepare(cStatement).setConsistencyLevel(consistencyLevel).bind());
                            }
                            consistencyLevel = newConLvl;
                        }
                    } else if(canExecuteConcurrently(statement)) {
                        concurrentStatements.add(statement);

                        if(concurrentStatements.size() >= CONCURRENT_BATCH_SIZE) {
                            //This is where we'd execute concurrently, but it isn't immediately clear how that would work.
                            System.err.println("Unimplemented codepath - async import");
                            throw new NotImplementedException();
                        }
                    } else {
                        for(String cStatement : concurrentStatements) {
                            _session.execute(_session.prepare(cStatement).setConsistencyLevel(consistencyLevel).bind());
                        }
                        concurrentStatements.clear();
                        _session.execute(_session.prepare(statement).setConsistencyLevel(consistencyLevel).bind());
                    }
                    statement = "";
                    cnt++;
                    if (cnt % DOT_EVERY == 0) {
                        logQuiet(".");
                    }
                }
                importLine = importFileReader.readLine();
            }
            //We've gone through the whole import file - execute any remaining statements (concurrently if possible)
            for(String cStatement : concurrentStatements) {
                _session.execute(_session.prepare(cStatement).setConsistencyLevel(consistencyLevel).bind());
            }
            concurrentStatements.clear();
            if(!statement.isEmpty()) {
                _session.execute(_session.prepare(statement).setConsistencyLevel(consistencyLevel).bind());
            }
            if(cnt > DOT_EVERY) {
                logQuiet("\n");
            }
        } catch(IOException ex) {
            System.err.println("Failed to import data");
            ex.printStackTrace();
        } finally {
            if(importFileReader != null) {
                try {
                    importFileReader.close();
                } catch (IOException e) {
                    //nop
                }
            }
        }
    }

    private static CassandraDumpJ parseCommandLineOptionsToInstance(String[] args) {
        CassandraDumpJ instance = new CassandraDumpJ();
        CommandLineParser parser = new DefaultParser();

        HelpFormatter helpFormatter = new HelpFormatter();
        String helpHeader = "A data exporting tool for Cassandra inspired from mysqldump, with some added slice and dice capabilities.\n\n";
        String helpFooter = "";

        CommandLine cmd = null;
        try {
            cmd = parser.parse(cli_parser, args);
            if(cmd.hasOption("connect-timeout")) {
                try {
                    instance.CONNECT_TIMEOUT = Integer.parseInt(cmd.getOptionValue("connect-timeout"));
                } catch (NumberFormatException e) {
                    throw new ParseException("arg 'connect-timeout' requires an integer value, but got '"+cmd.getOptionValue("connect-timeout")+"'");
                }
            }
            if(cmd.hasOption("cf")) {
                instance.CF = cmd.getOptionValues("cf");
            }
            if(cmd.hasOption("export-file")) {
                instance.EXPORT_FILE = new File(cmd.getOptionValue("export-file"));
                if(! instance.EXPORT_FILE.getParentFile().exists() || ! instance.EXPORT_FILE.getParentFile().isDirectory()) {
                    throw new ParseException("arg 'export-file' specifies a path whose parent does not exist or is not a directory: '" + cmd.getOptionValue("export-file") + "'");
                }
            }
            if(cmd.hasOption("filter")) {
                instance.FILTER = cmd.getOptionValues("filter");
            }
            if(cmd.hasOption("host")) {
                instance.HOST = cmd.getOptionValue("host");
            }
            if(cmd.hasOption("port")) {
                try {
                    instance.PORT = Integer.parseInt(cmd.getOptionValue("port"));
                } catch (NumberFormatException e) {
                    throw new ParseException("arg 'port' requires an integer value, but got '"+cmd.getOptionValue("port")+"'");
                }
            }
            if(cmd.hasOption("import-file")) {
                instance.IMPORT_FILE = new File(cmd.getOptionValue("import-file"));
                if(! instance.IMPORT_FILE.exists() || ! instance.IMPORT_FILE.isFile()) {
                    throw new ParseException("arg 'import-file' specifies a path that is not a file or does not exist: '" + cmd.getOptionValue("import-file") + "'");
                }
            }
            if(cmd.hasOption("keyspace")) {
                instance.KEYSPACE = cmd.getOptionValues("keyspace");
            }
            if(cmd.hasOption("exclude-cf")) {
                instance.EXCLUDE_CF = cmd.getOptionValues("exclude-cf");
            }
            instance.NO_CREATE = cmd.hasOption("no-create");
            instance.NO_INSERT = cmd.hasOption("no-insert");
            if(cmd.hasOption("password")) {
                instance.PASSWORD = cmd.getOptionValue("password");
            }
            if(cmd.hasOption("protocol-version")) {
                try {
                    instance.PROTOCOL_VERSION = Integer.parseInt(cmd.getOptionValue("protocol-version"));
                } catch (NumberFormatException e) {
                    throw new ParseException("arg 'protocol-version' requires an integer value, but got '"+cmd.getOptionValue("protocol-version")+"'");
                }
            }
            instance.QUIET = cmd.hasOption("quiet");
            instance.SYNC = cmd.hasOption("sync");
            if(cmd.hasOption("username")) {
                instance.USERNAME = cmd.getOptionValue("username");
            }
            if(cmd.hasOption("limit")) {
                try {
                    instance.LIMIT = Integer.parseInt(cmd.getOptionValue("limit"));
                } catch (NumberFormatException e) {
                    throw new ParseException("arg 'limit' requires an integer value, but got '"+cmd.getOptionValue("limit")+"'");
                }
            }
            instance.SSL = cmd.hasOption("ssl");
            if(cmd.hasOption("certfile")) {
                instance.CERTFILE = new File(cmd.getOptionValue("certfile"));
                if(!instance.CERTFILE.exists() || !instance.CERTFILE.isFile()) {
                    throw new ParseException("arg 'certfile' specifies a path that is not a file or does not exist: '" + cmd.getOptionValue("certfile") + "'");
                }
            }
            if(cmd.hasOption("userkey")) {
                instance.USERKEY = new File(cmd.getOptionValue("userkey"));
                if(!instance.USERKEY.exists() || !instance.USERKEY.isFile()) {
                    throw new ParseException("arg 'iuserkey' specifies a path that is not a file or does not exist: '" + cmd.getOptionValue("userkey") + "'");
                }
            }
            if(cmd.hasOption("usercert")) {
                instance.USERCERT = new File(cmd.getOptionValue("usercert"));
                if(!instance.USERCERT.exists() || !instance.USERCERT.isFile()) {
                    throw new ParseException("arg 'usercert' specifies a path that is not a file or does not exist: '" + cmd.getOptionValue("usercert") + "'");
                }
            }

            if(instance.IMPORT_FILE == null && instance.EXPORT_FILE == null) {
                throw new ParseException("--import-file or --export-file must be specified");
            }

            if(instance.USERKEY != null ^ instance.USERCERT != null) { //Rarely-used Java XOR operation. Requires both/neither
                throw new ParseException("--userkey and --usercert must both be provided");
            }

            if(instance.IMPORT_FILE != null && instance.EXPORT_FILE != null) {
                throw new ParseException("--import-file and --export-file can't be specified at the same time");
            }

            if(instance.SSL && instance.CERTFILE == null) {
                throw new ParseException("--certfile must also be specified when using --ssl");
            }

            /*
            Unsupported options follow here:
            I'm porting this library from Python to Java, but I'll be the first to admit that I am doing this
             - For practical reasons
             - With limited time/resources
            Anything that I identify as I work on the port that I am unsure of or unable to test will end up here
             */
            //protocol-version 1 uses some JSON object in the python for authentication, whereas greater-than-1 uses a PlainTextAuthProvider object
            //I am only aware of how to use the PlaintextAuthProvider via The DataStax Cassandra Library in Java - therefore, I'm disabling protocol-version 1.
            if(instance.PROTOCOL_VERSION == 1) {
                throw new ParseException("UNIMPLEMENTED FEATURE: --protocol-version 1 is unimplemented and, frankly, probably won't ever work. Please specify a protocol-version of 2 to continue");
            }
            //Java's SSL setup is a bit of a nightmare and I'm not presently equipped to test it even if I attempted to implement it.
            if(instance.SSL) {
                throw new ParseException("UNIMPLEMENTED FEATURE: The code for activating SSL-based authentication has not yet been implemented");
            }
            //DataStax' Python driver has a convenience function for executing queries concurrently - the Java driver does not...
            //...or, at least, it isn't clear if it does it automatically. Suffice it to say, it would appear an ExecutorService
            //might be necessary, and while that'd be nice to have, it's not absolutely necessary for this to work, so
            //I'm skipping it for now. Read more here: https://docs.datastax.com/en/dev-app-drivers/docs/managing/manage-concurrency.html
            if(!instance.SYNC && instance.IMPORT_FILE != null) {
                throw new ParseException("UNIMPLEMENTED FEATURE: Async import is not presently supported - please specify --sync for now");
            }
        } catch (ParseException e) {
            System.out.println("Error: " + e.getMessage());
            helpFormatter.printHelp("java -jar cassandradumpJ-*-jar-with-dependencies.jar", helpHeader, cli_parser, helpFooter);
            System.exit(1);
        }
        return instance;
    }

    private static Options constructParserArgs() {
        Options parser = new Options();
        parser.addOption(new Option(null, "connect-timeout", true, "set timeout for connecting to the cluster (in seconds)"));
        parser.addOption(new Option(null, "cf", true, "export a column family. The name must include the keyspace, e.g. \"system.schema_columns\". Can be specified multiple times"));
        parser.addOption(new Option(null, "export-file", true, "export data to the specified file"));
        parser.addOption(new Option(null, "filter", true, "export a slice of a column family according to a CQL filter. This takes essentially a typical SELECT query stripped of the initial \"SELECT ... FROM\" part (e.g. \"system.schema_columns where keyspace_name ='OpsCenter'\", and exports only that data. Can be specified multiple times"));
        parser.addOption(new Option(null, "host", true, "the address of a Cassandra node in the cluster (localhost if omitted)"));
        parser.addOption(new Option(null, "port", true, "the port of a Cassandra node in the cluster (9042 if omitted)"));
        parser.addOption(new Option(null, "import-file", true, "import data from the specified file"));
        parser.addOption(new Option(null, "keyspace", true, "export a keyspace along with all its column families. Can be specified multiple times"));
        parser.addOption(new Option(null, "exclude-cf", true, "when using --keyspace, specify column family to exclude.  Can be specified multiple times"));
        parser.addOption(new Option(null, "no-create", false, "don't generate create (and drop) statements"));
        parser.addOption(new Option(null, "no-insert", false, "don't generate insert statements"));
        parser.addOption(new Option(null, "password", true, "set password for authentication (only if protocol-version is set)"));
        parser.addOption(new Option(null, "protocol-version", true, "set protocol version (required for authentication)"));
        parser.addOption(new Option(null, "quiet", false, "quiet progress logging"));
        parser.addOption(new Option(null, "sync", false, "import data in synchronous mode (default asynchronous)"));
        parser.addOption(new Option(null, "username", true, "set username for auth (only if protocol-version is set)"));
        parser.addOption(new Option(null, "limit", true, "set number of rows return limit"));
        parser.addOption(new Option(null, "ssl", false, "enable ssl connection to Cassandra cluster.  Must also set --certfile."));
        parser.addOption(new Option(null, "certfile", true, "ca cert file for SSL.  Assumes --ssl."));
        parser.addOption(new Option(null, "userkey", true, "user key file for client authentication.  Assumes --ssl."));
        parser.addOption(new Option(null, "usercert", true, "user cert file for client authentication.  Assumes --ssl."));
        return parser;
    }
}

