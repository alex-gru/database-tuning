
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class InsertData {
    static Connection con;
    static BaseConnection baseCon;
    static CopyManager manager;
	public static void main(String[] args) {
        try {
            con = new DBConnector().getConnection();
            baseCon = (BaseConnection) con;
            manager = new CopyManager(baseCon);
            long start = System.nanoTime();
            insertUsingCopy();
            long finish = System.nanoTime();
            System.out.println("Insertion took " + ((finish - start) / Math.pow(10, 9)) + " seconds.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void insertUsingCopy() throws SQLException, IOException {
        manager.copyIn("COPY \"Accounts\" from STDIN ", new FileInputStream("account.tsv"));
    }

}
