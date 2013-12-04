package dbt02;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnector {
    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String host = "dumbo.cosy.sbg.ac.at";
//        String host = "localhost";
        String port = "5432";
        String database = "TDBWS2013-GruppeB";
//        String database = "postgres";
        String pwd = "dahd8Ce4";
//        String pwd = "ev2bjk..";
        String user = "TDBmkotoy";
//        String user = "postgres";
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(url, user, pwd);
    }
}
