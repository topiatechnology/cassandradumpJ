package com.topiatechnology.cassandradumpJ;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class CassandraDumpJTest {
    protected final Log _log = LogFactory.getLog(this.getClass());

    private static String _cassandraAddress;
    private static int _cassandraPort;

    protected Cluster _cluster;
    protected Session _session;

    //Test data to manually load into Cassandra
    private static final Map<String, List<String>> testKeyspaceTables = new HashMap<>();
    private static final Map<String, String> testKeyspaceTableCreateStatements = new HashMap<>();

    static {
        testKeyspaceTables.put("keyspace_1", new ArrayList<>());
        //We'll replace '?' with the key
        testKeyspaceTables.get("keyspace_1").add("table_1");
        testKeyspaceTableCreateStatements.put("keyspace_1.table_1", "CREATE TABLE IF NOT EXISTS keyspace_1.table_1 (" +
                "  id uuid PRIMARY KEY, " +
                "  string text, " +
                "  integer int, " +
                "  long bigint);");
        testKeyspaceTables.get("keyspace_1").add("table_2");
        testKeyspaceTableCreateStatements.put("keyspace_1.table_2", "CREATE TABLE IF NOT EXISTS keyspace_1.table_2 (" +
                "  id uuid PRIMARY KEY, " +
                "  count counter);");
        testKeyspaceTables.put("keyspace_2", new ArrayList<>());
        testKeyspaceTables.get("keyspace_2").add("table_3");
        testKeyspaceTableCreateStatements.put("keyspace_2.table_3", "CREATE TABLE IF NOT EXISTS keyspace_2.table_3 (" +
                "  id uuid PRIMARY KEY, " +
                "  string text, " +
                "  integer int, " +
                "  long bigint);");

    }


    @BeforeClass
    public static void setupClass() throws Exception {
        _cassandraAddress = "127.0.0.1";
        _cassandraPort = 9142;
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(60000);
    }

    @Before
    public void setup() throws Exception {
        _log.info ( "Connecting to Cluster" ) ;
        _cluster = Cluster.builder().addContactPoint(_cassandraAddress).withPort(_cassandraPort).build();
        _log.info ( "Establishing Session" ) ;
        _session = _cluster.newSession();

        _log.info("Creating " + testKeyspaceTables.keySet().size() + " keyspaces");
        for(String keyspaceName : testKeyspaceTables.keySet()) {
            if ( keyspaceName.length() > 48 ) {
                keyspaceName = keyspaceName.substring(keyspaceName.length() - 48);
            }
            _session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':1}");
        }
        _log.info("Creating " + testKeyspaceTableCreateStatements.keySet().size() + " tables");
        for(String tableCQL : testKeyspaceTableCreateStatements.values()) {
            PreparedStatement createStatement = _session.prepare(tableCQL);
            BoundStatement boundStatement = createStatement.bind();
            _session.execute(boundStatement);
        }
        insertTestTableData();

        _log.info ( "Setup Complete") ;
    }

    @After
    public void teardown() throws Exception {
        _log.info ( "Tearing Down" );
        _session.close();
        _cluster.close();

        _session = null;
        _cluster = null;

        try {
            _log.info ( "Cleaning Up" );
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        } catch (OperationTimedOutException e) {
            e.printStackTrace();
        }

        _log.info ( "Done" );
    }

    public void insertTestTableData() {
        _log.info("Inserting test data");
        BoundStatement boundStatement = _session.prepare(
                "INSERT INTO keyspace_1.table_1 (id, string, integer, long) " +
                    "VALUES (?, ?, ?, ?);")
                .bind(
                UUID.randomUUID(),
                "testStrData",
                7,
                8888L
        );
        _session.execute(boundStatement);

        UUID counterUUID = UUID.randomUUID();
        boundStatement = _session.prepare( //Init the counter to 1
                        "UPDATE keyspace_1.table_2 SET count = count + 1 WHERE id = ?;")
                .bind(
                        counterUUID
                );
        _session.execute(boundStatement);

        boundStatement = _session.prepare( //Increment the counter to 2 just for funsies
                        "UPDATE keyspace_1.table_2 SET count = count + 1 WHERE id = ?;")
                .bind(
                        counterUUID
                );
        _session.execute(boundStatement);

        boundStatement = _session.prepare(
                    "INSERT INTO keyspace_2.table_3 (id, string, integer, long) " +
                        "VALUES (?, ?, ?, ?);")
            .bind(
                    UUID.randomUUID(),
                    "testStrData2",
                    9,
                    4444L
            );
        _session.execute(boundStatement);
    }

    @Test
    public void testExportAll() {
        File exportFile = new File("target/testExportAllFile1.txt");
        if(exportFile.exists()) {
            exportFile.delete();
        }
        String[] args = {
                "--host", _cassandraAddress,
                "--port", "" + _cassandraPort,
                "--export-file", exportFile.getAbsolutePath()};
        CassandraDumpJ.main(args);
        Assert.assertTrue(exportFile.exists());
        Set<String> allTestKeyspaces = new HashSet<>(testKeyspaceTables.keySet());
        Set<String> allTestKeyspaceTables = new HashSet<>(testKeyspaceTableCreateStatements.keySet());
        //Scan the file
        // - removing keyspaces from the "allTestKeyspaces" set as we find their "CREATE" statements
        // - removing tables from the "allTestKeyspaceTables" set as we find their "CREATE" statements
        //We aren't testing the inserts of data because, frankly, I cannot think of a good way to do that.
        //We'll pass the test if the Sets are empty at the end.
        BufferedReader exportedFileReader = null;
        try {
            exportedFileReader = new BufferedReader(new FileReader(exportFile));
            String line = exportedFileReader.readLine();
            while (line != null) {
                if(line.startsWith("CREATE KEYSPACE ")) {
                    String[] parts = line.split(" ");
                    String keyspaceName = parts[2];
                    allTestKeyspaces.remove(keyspaceName);
                } else if(line.startsWith("CREATE TABLE ")) {
                    String[] parts = line.split(" ");
                    String tableName = parts[2];
                    allTestKeyspaceTables.remove(tableName);
                }
                line = exportedFileReader.readLine();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(exportedFileReader != null) {
                try {
                    exportedFileReader.close();
                } catch (IOException e) {
                    //nop
                }
            }
        }
        Assert.assertTrue(allTestKeyspaces.isEmpty());
        Assert.assertTrue(allTestKeyspaceTables.isEmpty());
    }

    @Test
    public void testExportSingleKeyspace() {
        File exportFile = new File("target/testExportOneKeyspaceFile1.txt");
        if(exportFile.exists()) {
            exportFile.delete();
        }
        String[] args = {
                "--host", _cassandraAddress,
                "--port", "" + _cassandraPort,
                "--keyspace", "keyspace_1",
                "--export-file", exportFile.getAbsolutePath()};
        CassandraDumpJ.main(args);
        Assert.assertTrue(exportFile.exists());
        Set<String> allTestKeyspaces = new HashSet<>(Collections.singleton("keyspace_1"));
        Set<String> allTestKeyspaceTables = new HashSet<>();
        allTestKeyspaceTables.add("keyspace_1.table_1");
        allTestKeyspaceTables.add("keyspace_1.table_2");
        //Scan the file
        // - removing keyspaces from the "allTestKeyspaces" set as we find their "CREATE" statements
        // - removing tables from the "allTestKeyspaceTables" set as we find their "CREATE" statements
        //We aren't testing the inserts of data because, frankly, I cannot think of a good way to do that.
        //We'll pass the test if the Sets are empty at the end.
        BufferedReader exportedFileReader = null;
        try {
            exportedFileReader = new BufferedReader(new FileReader(exportFile));
            String line = exportedFileReader.readLine();
            while (line != null) {
                if(line.startsWith("CREATE KEYSPACE ")) {
                    String[] parts = line.split(" ");
                    String keyspaceName = parts[2];
                    allTestKeyspaces.remove(keyspaceName);
                } else if(line.startsWith("CREATE TABLE ")) {
                    String[] parts = line.split(" ");
                    String tableName = parts[2];
                    allTestKeyspaceTables.remove(tableName);
                }
                line = exportedFileReader.readLine();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(exportedFileReader != null) {
                try {
                    exportedFileReader.close();
                } catch (IOException e) {
                    //nop
                }
            }
        }
        Assert.assertTrue(allTestKeyspaces.isEmpty());
        Assert.assertTrue(allTestKeyspaceTables.isEmpty());
    }

    @Test
    public void testExportAndImportSingleKeyspace() {
        File exportFile = new File("target/testExportOneKeyspaceFile2.txt");
        if(exportFile.exists()) {
            exportFile.delete();
        }
        String[] args = {
                "--host", _cassandraAddress,
                "--port", "" + _cassandraPort,
                "--keyspace", "keyspace_1",
                "--export-file", exportFile.getAbsolutePath()};
        CassandraDumpJ.main(args);
        Assert.assertTrue(exportFile.exists());

        args = new String[7];
        args[0] = "--host"; args[1] = _cassandraAddress;
        args[2] = "--port"; args[3] = "" + _cassandraPort;
        args[4] = "--import-file"; args[5] = exportFile.getAbsolutePath();
        args[6] = "--sync";
        //Ok, let's drop keyspace_1 and confirm that it no longer exists.
        Assert.assertNotNull(_cluster.getMetadata().getKeyspace("keyspace_1"));
        _session.execute("DROP KEYSPACE keyspace_1");
        Assert.assertNull(_cluster.getMetadata().getKeyspace("keyspace_1"));
        //Now we'll import again.
        CassandraDumpJ.main(args);
        //And make sure stuff exists again
        KeyspaceMetadata keyspace1 = _cluster.getMetadata().getKeyspace("keyspace_1");
        Assert.assertNotNull(keyspace1);
        Assert.assertNotNull(keyspace1.getTable("table_1"));
        Assert.assertNotNull(keyspace1.getTable("table_2"));
        ResultSet rs = _session.execute("SELECT string FROM keyspace_1.table_1");
        String result = rs.one().get(0, String.class);
        Assert.assertEquals("testStrData", result);
    }

    @Test
    public void testIsCodecRegAddingExtraQuotes() {
        CodecRegistry codecRegistry = _cluster.getConfiguration().getCodecRegistry();
        Object value = "foo";
        String encodedValue = codecRegistry.codecFor(DataType.ascii()).format(value);
        Assert.assertEquals("'" + value + "'", encodedValue);
    }

}