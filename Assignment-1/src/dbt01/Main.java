package dbt01;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;

public class Main {
    static Connection con;
    static BaseConnection baseCon;
    static CopyManager manager;

    /*
    Don't forget to store the postgresql driver and files "auth.tsv" and "auth_10000.tsv" in your working
    directory. The latter one contains 10,000 lines of the former one.
    */

    public static void main(String[] args) throws SQLException {
        try {
            con = DBConnector.getConnection();
            baseCon = (BaseConnection) con;
            manager = new CopyManager(baseCon);
            dropAuthTable();
            addAuthTable();

            long start = System.nanoTime();
//            insertLineByLine(10000);
//            insertMultiple(10000);
            insertUsingCopy();
            long finish = System.nanoTime();
            System.out.println("Insertion took " + ((finish - start) / Math.pow(10, 9)) + " seconds.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printAllTuples(String tableName) throws SQLException {
        String command = "SELECT * FROM " + tableName;
        ResultSet rs = con.createStatement().executeQuery(command);
        System.out.println("All tuples in " + tableName + ":");
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
    }

    private static void addAuthTable() {
        String command = "CREATE TABLE Auth(Name varchar(49), PubID varchar(129))";
        try {
            con.createStatement().execute(command);
        } catch (SQLException e) {
            //ignore
        }
    }

    private static void dropAuthTable() {
        String command = "DROP TABLE Auth";
        try {
            con.createStatement().execute(command);
        } catch (SQLException e) {
            //ignore
        }
    }

    private static void insertLineByLine(int count) throws IOException, SQLException {
        BufferedReader in = new BufferedReader(new FileReader("auth.tsv"));
        for (int i = 0; i < count; i++) {
            String line = in.readLine();
            String[] values = line.split("\t");
            String command = "INSERT INTO AUTH VALUES('" + escape(values[0]) + "', '" + escape(values[1]) + "')";
            con.createStatement().execute(command);
        }
    }

    private static void insertMultiple(int count) throws IOException, SQLException {
        BufferedReader in = new BufferedReader(new FileReader("auth.tsv"));
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < count; i++) {
            String line = in.readLine();
            String[] values = line.split("\t");
            string.append("INSERT INTO AUTH VALUES('");
            string.append(escape(values[0]));
            string.append("', '");
            string.append(escape(values[1]));
            string.append("'); ");
        }
        string.replace(string.length() - 1, string.length() - 1, "");
        con.createStatement().execute(string.toString());
    }

    private static void insertUsingCopy() throws SQLException, IOException {
        manager.copyIn("COPY Auth from STDIN", new FileInputStream("auth_10000.tsv"));
    }

    private static String escape(String val) {
        return val.replace("\'", "\'\'");
    }

}