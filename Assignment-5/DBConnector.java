

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnector {
    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        String host = "dumbo.cosy.sbg.ac.at";
        String port = "5432";
        String database = "TDBWS2013-GruppeB";
        String pwd = "dahd8Ce4";
        String user = "TDBmkotoy";
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(url, user, pwd);
    }
}
