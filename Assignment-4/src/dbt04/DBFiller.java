package dbt04;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * User: alexgru
 */
public class DBFiller {

    public static void fillDB(Connection conn, File[] files) throws SQLException, IOException {
        BaseConnection baseCon = (BaseConnection) conn;
        CopyManager manager = new CopyManager(baseCon);
        for (int i = 0; i < files.length; i++) {
            manager.copyIn("COPY publ from STDIN", new FileInputStream(files[i]));
            System.out.println("SUCCESSFULLY COPIED " + Main.NUMBERLINESSPLIT + " ENTRIES");
        }

        System.out.println("\t\t FINISHED.");
    }
}
