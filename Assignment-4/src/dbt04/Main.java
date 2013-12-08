package dbt04;

import org.postgresql.util.PSQLException;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * User: alexgru
 */
public class Main {
    public static final int TOTALNUMBEROFLINES = 1233216;
    public static final int NUMBERLINESSPLIT = 10000;
    private static final String notExistingValue = "j31j0fj9u309u5r";
    private static String[] pubidConds;
    private static String[] booktitleConds;
    private static String[] yearConds;
    private static final String[][] conds = new String[][]{pubidConds, booktitleConds, yearConds};
    private static final String[] attributes = new String[]{"pubID", "booktitle", "year"};
    private static final String[] independentAttributes = new String[]{"type", "year", "title"};

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        final Connection connection = DBConnector.getConnection();
//        dropPublnewTable(connection);
//        addPublnewTable(connection);
//        File[] files = splitFileIntoMore("publ.tsv", NUMBERLINESSPLIT);
//        DBFiller.fillDB(connection, files);
//        loadConditionsPseudoRandomlyIntoFile(connection, "booktitle");
//        loadConditionsPseudoRandomlyIntoFile(connection, "pubid");
//        loadConditionsPseudoRandomlyIntoFile(connection, "year");
        testQueryPerformance(connection, 50, "year", "title", "btree", true);
        testQueryPerformance(connection, 50, "year", "title", "btree", false);
        testQueryPerformance(connection, 50, "year", "title", "hash", false);
        testQueryPerformance(connection, 50, "year", "title", null, false);

        testQueryPerformance(connection, 200, "booktitle", "year", "btree", true);
        testQueryPerformance(connection, 200, "booktitle", "year", "btree", false);
        testQueryPerformance(connection, 200, "booktitle", "year", "hash", false);
        testQueryPerformance(connection, 200, "booktitle", "year", null, false);

        testQueryPerformance(connection, 300, "pubid", "type", "btree", true);
        testQueryPerformance(connection, 300, "pubid", "type", "btree", false);
        testQueryPerformance(connection, 300, "pubid", "type", "hash", false);
        testQueryPerformance(connection, 300, "pubid", "type", null, false);
    }

    private static void loadConditionsPseudoRandomlyIntoFile(Connection conn, String attribute) throws IOException, SQLException {
        String query = "SELECT DISTINCT " + attribute + " FROM PUBLNEW";
        FileWriter out = new FileWriter(new File(attribute + "-conds.txt"));
        System.out.println("Execute query: " + query);

        ResultSet resultSet = conn.createStatement().executeQuery(query);
        ArrayList<String> values = new ArrayList<String>();
        while (resultSet.next()) {
            values.add(resultSet.getString(1));
        }
        int count = values.size();
        out.write("");
        while (!values.isEmpty()) {
            String val = values.remove((int) (Math.random() * (values.size() - 1)));
            if (val == null) {
                continue;
            }
            out.append(val + "\n");
        }
        out.close();
        System.out.println("Successfully, (pseudo) randomly copied " + count + " entries into file " + attribute
                + "-conds.txt");
    }

    private static void testQueryPerformance(Connection conn, int numberRunsPerQuery, String attribute,
                                             String independentAttribute, String dataStructure,
                                             boolean clustered) throws SQLException, IOException {
        String filename = "attribute_" + attribute + "_index_" + dataStructure + "_clustered_" + clustered + ".txt";
        FileWriter log = new FileWriter(new File(filename));
        log.write("------------------ QUERY PERFORMANCE TEST ------------------");
        System.out.println("------------------ QUERY PERFORMANCE TEST ------------------");
        log.append("\nQueried attribute: " + attribute);
        log.append(" | Number of query runs: " + numberRunsPerQuery);
        log.append(" | INDEX: ");
        if (dataStructure != null) {
            log.append(dataStructure + ", clustered: " + clustered);
        } else {
            log.append("no index, TABLE SCAN");
        }
        log.append("\n------------------------------------------------------------");

        double totalTime = 0;
        String[] conds = new String[numberRunsPerQuery];
        BufferedReader in = new BufferedReader(new FileReader(new File(attribute + "-conds.txt")));
        for (int i = 0; i < numberRunsPerQuery; i++) {
            conds[i] = in.readLine();
        }
        in.close();

//        conn.createStatement().execute("DROP INDEX IF EXISTS " + attribute + "_idx");
        String index = null;
        if (dataStructure != null) {
            index = "CREATE INDEX " + attribute + "_idx " + "ON publnew USING " + dataStructure + "(" +
                    attribute + ");";
        }

        String cluster = null;
        if (clustered) {
            cluster = "CLUSTER publ USING " + attribute + "_idx";
        } else if (dataStructure != null) {
            try {
                conn.createStatement().execute("CREATE INDEX " + independentAttribute + "_idx ON publnew ("
                        + independentAttribute + ")");
            } catch (PSQLException e) {
                System.out.println("index " + independentAttribute + "_idx already exists. move on...");
            }
            cluster = "CLUSTER publ USING " + independentAttribute + "_idx";
        }
        if (index != null) {
            log.append("\nINDEX-statement: " + index);
            try {
                conn.createStatement().execute(index);
            } catch (PSQLException e) {
                System.out.println("index " + attribute + "_idx already exists. move on...");
            }
        }
        if (cluster != null) {
            log.append("\nCLUSTER: " + cluster);
            conn.createStatement().execute(cluster);
        }

        long start = System.nanoTime();

        for (int i = 0; i < conds.length; i++) {
            String query = "SELECT * FROM publnew WHERE " + attribute + "='" + conds[i] + "'";
            log.append("\n" + query);
            conn.createStatement().executeQuery(query);
        }
        long finish = System.nanoTime();
        double time = (finish - start) / Math.pow(10, 9);
        totalTime += time;

        log.append("\n------------------------------------------------------------");
        log.append("\nQueries took " + time + " seconds.");
        double throughput = numberRunsPerQuery / time;
        log.append("\nTHROUGHPUT: " + throughput + " queries per second");
        log.close();
        System.out.println("Performance test results can be found in " + filename);
    }

    private static File[] splitFileIntoMore(String fileName, int maxNumberOfRows) throws IOException {
        File[] files = new File[TOTALNUMBEROFLINES / maxNumberOfRows + 1];
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        boolean finished = false;
        int count = 0;
        int line = 0;
        while (!finished) {
            File file = new File(fileName + "-" + count);
            file.createNewFile();
            files[count] = file;

            FileWriter out = new FileWriter(file);
            for (int i = 0; i < maxNumberOfRows; i++) {
                String lineString = in.readLine();
                if (lineString == null || lineString.equals("null")) {
                    finished = true;
                    break;
                }
                out.append(lineString + "\n");
            }
            out.close();
            line += maxNumberOfRows;
            count++;
            if (line >= TOTALNUMBEROFLINES) {
                finished = true;
            }
        }
        return files;
    }

    private static void addPublnewTable(Connection conn) throws SQLException {
        String command = "CREATE TABLE Publnew(pubID varchar(129), type varchar(13), title varchar(700), " +
                "booktitle varchar(132), year varchar(4), publisher varchar(196))";
        conn.createStatement().execute(command);
    }

    private static void dropPublnewTable(Connection conn) throws SQLException {
        String command = "DROP TABLE IF EXISTS Publnew";
        conn.createStatement().execute(command);
    }
}
