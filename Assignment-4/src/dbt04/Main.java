package dbt04;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * User: alexgru
 */
public class Main {
    public static final int TOTALNUMBEROFLINES = 1233216;
    public static final int NUMBERLINESSPLIT = 10000;
    private static final String notExistingValue = "j31j0fj9u309u5r";
    private static final String[] pubidConds = new String[]{"books/eh/campbell84/CampbellH84",
            "books/idea/encyclopedia2005/GelmanTFC05", "books/idea/encyclopedia2005/PotgieterAB05",
            "books/idea/encyclopedia2005/VerburgAR05", "conf/mm/Li02", "conf/normas/Kibble05",
            "journals/corr/cs-DB-0306013", "journals/gpem/Banzhaf03", "journals/kbs/NakakojiYO00",
            "journals/pami/KirbyS90", notExistingValue, "journals/csda/CoppiD06", "journals/ijstm/GormanS00",
            "journals/endm/AcharyaJ03", "series/vdi/Bogel07"};
    private static final String[] booktitleConds = new String[]{"Modern Database Systems",
            "Specification and Verification of Concurrent Systems", notExistingValue, "Temporal Databases",
            "The Computer Science and Engineering Handbook",
            "Tree Automata and Languages", "Encyclopedia of Information Science and Technology (IV)",
            "Web & Datenbanken", "ACSD", "The Industrial Information Technology Handbook", "The Compiler Design Handbook",
            "Object-Oriented Concepts, Databases, and Applications", "On the Construction of Programs",
            "Web Engineering: Systematische Entwicklung von Web-Anwendungen", "Implementations of Prolog",
            "Logic Programming: Formal Methods and Practical Applications"};
    private static final String[] yearConds = new String[]{"1995", "1990", "2005", "1984", "2000", "1980", "2002", "1986",
            "1997", "2003", "1987", "2001", "1983", "1988", "1992"};
    private static final String[][] conds = new String[][]{pubidConds, booktitleConds, yearConds};
    private static final String[] attributes = new String[]{"pubID", "booktitle", "year"};
    private static final String[] independentAttributes = new String[]{"type", "year", "title"};

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        final Connection connection = DBConnector.getConnection();
//        dropPublTable(connection);
//        addPublTable(connection);
//        File[] files = splitFileIntoMore("publ.tsv", NUMBERLINESSPLIT);
//        DBFiller.fillDB(connection, files);
        testQueryPerformance(connection, 10, "hash", false);
    }

    private static void testQueryPerformance(Connection conn, int numberRunsPerQuery, String dataStructure,
                                             boolean clustered) throws SQLException {
        System.out.println("----- QUERY PERFORMANCE TEST -----");
        double totalTime = 0;

        for (int i = 0; i < conds.length; i++) {
            String currAttribute = attributes[i];
            conn.createStatement().execute("DROP INDEX IF EXISTS " + currAttribute + "_idx");
            String index = "CREATE INDEX " + currAttribute + "_idx " + "ON publ USING " + dataStructure + "(" +
                    currAttribute + ");";
            String cluster = null;
            if (clustered) {
                cluster = "ALTER TABLE publ CLUSTER ON " + currAttribute + "_idx";
            } else {
                conn.createStatement().execute("DROP INDEX IF EXISTS " + independentAttributes[i] + "_idx");
                conn.createStatement().execute("CREATE INDEX " + independentAttributes[i] + "_idx ON publ USING "
                        + dataStructure + " (" + currAttribute + ")");
                cluster = "ALTER TABLE publ CLUSTER ON " + independentAttributes[i] + "_idx";
            }
            System.out.println("\t" + index);
            System.out.println("\t" + cluster);
            conn.createStatement().execute(index);
            conn.createStatement().execute(cluster);

            long start = System.nanoTime();
            for (int j = 0; j < numberRunsPerQuery; j++) {
                String query = "SELECT * FROM Publ WHERE " + currAttribute + "='" + conds[i][j] + "'";
                System.out.println(query);
                conn.createStatement().executeQuery(query);
            }
            long finish = System.nanoTime();
            double time = (finish - start) / Math.pow(10, 9);
            totalTime += time;
            System.out.println("Query took " + time + " seconds.");
        }
        System.out.println("Total time [cluster: " + clustered + ", " + dataStructure + "]: " + totalTime + " seconds.");

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

    private static void addPublTable(Connection conn) throws SQLException {
        String command = "CREATE TABLE Publ(pubID varchar(129), type varchar(13), title varchar(700), " +
                "booktitle varchar(132), year varchar(4), publisher varchar(196))";
        conn.createStatement().execute(command);
    }

    private static void dropPublTable(Connection conn) throws SQLException {
        String command = "DROP TABLE IF EXISTS Publ";
        conn.createStatement().execute(command);
    }
}
