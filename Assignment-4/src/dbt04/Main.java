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

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        final Connection connection = DBConnector.getConnection();
//        dropPublTable(connection);
//        addPublTable(connection);
//        File[] files = splitFileIntoMore("publ.tsv", NUMBERLINESSPLIT);
//        DBFiller.fillDB(connection, files);
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
