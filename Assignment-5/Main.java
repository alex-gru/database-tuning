

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;


public class Main{
	
	static Connection connection;
	
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
    	connection = DBConnector.getConnection();
    	String query_1 = "ANALYZE EXPLAIN SELECT name,title FROM Auth, Publ WHERE Auth.pubID=Publ.pubID;";
    	String query_2 = "ANALYZE EXPLAIN SELECT title FROM Auth, Publ WHERE Auth.pubID=Publ.pubID AND Auth.name='Divesh Srivastava'";
    	String index_a2 = "CREATE UNIQUE INDEX publ_pubid_idx ON publ (pubid);";
    	String index_a3 = "CREATE INDEX publ_pubid_idx ON publ (pubid);\n"+
    					  "CREATE INDEX auth_pubid_idx ON auth (pubid);\n"+
    					  "CLUSTER publ USING publ_pubid_idx;\n"+
    					  "CLUSTER auth USING auth_pubid_idx;";
    	String index_b1 = "CREATE INDEX publ_pubid_idx ON publ (pubid);";
    	String index_b2 = "CREATE INDEX auth_pubid_idx ON auth (pubid);";
    	String index_b3 = "CREATE INDEX publ_pubid_idx ON publ (pubid);\n"+
    					  "CREATE INDEX auth_pubid_idx ON auth (pubid);";
    	String index_c2 = index_b2;
    	String index_c3 = index_a3;
        dropPublTable();
        addPublTable();
        dropAuthTable();
        addAuthTable();
        File[] files = {new File("publ.tsv"),new File("auth.tsv")};
        String[] tables = {"publ","auth"};
        fillDB(files,tables);
//        dropIndexes();
//        createIndexes(index_a3);
//        setJoinStrategies(true,false,false);
//        executeQuery(query_1);
//        executeQuery(query_2);
    }

    private static void dropIndexes() throws SQLException {
        connection.createStatement().execute("DROP INDEX IF EXISTS publ_pubid_idx;\n" +
        									 "DROP INDEX IF EXISTS auth_pubid_idx;");
        System.out.println("INDEXES DROPED");
    }
    
    private static void createIndexes(String index) throws SQLException{
    	connection.createStatement().execute(index);
    	System.out.println("INDEXES CREATED");
    }
    
    private static void addPublTable() throws SQLException {
        String command = "CREATE TABLE Publ(pubID varchar(129), type varchar(13), title varchar(700), " +
                "booktitle varchar(132), year varchar(4), publisher varchar(196))";
        connection.createStatement().execute(command);
        System.out.println("TABLE PUBL CREATED");
    }

    private static void dropPublTable() throws SQLException {
        String command = "DROP TABLE IF EXISTS Publ";
        connection.createStatement().execute(command);
        System.out.println("TABLE PUBL DROPED");
    }
    
    private static void addAuthTable() throws SQLException {
        String command = "CREATE TABLE Auth(name varchar(49), pubID varchar(129))";
        connection.createStatement().execute(command);
        System.out.println("TABLE AUTH CREATED");
    }

    private static void dropAuthTable() throws SQLException {
        String command = "DROP TABLE IF EXISTS Auth";
        connection.createStatement().execute(command);
        System.out.println("TABLE AUTH DROPED");
    }
    
    private static void fillDB(File[] files, String[] tables) throws SQLException, IOException {
        BaseConnection baseCon = (BaseConnection) connection;
        CopyManager manager = new CopyManager(baseCon);
        for (int i = 0; i < files.length; i++) {
            manager.copyIn("COPY "+tables[i]+" from STDIN", new FileInputStream(files[i]));
            System.out.println("SUCCESSFULLY COPIED "+files[i].getName());
        }
        System.out.println("\t\t FINISHED.");
    }
    
    private static void setJoinStrategies(boolean hashjoin, boolean mergejoin, boolean nestloop) throws SQLException{
    	connection.createStatement().execute("SET enable_hashjoin TO "+hashjoin+";\n"+
    										 "SET enable_mergejoin TO "+mergejoin+";\n"+
    										 "SET enable_nestloop TO "+nestloop+";");
    	System.out.println("SWITCHED JOIN STRATEGIES:\n"+
    					   "HASHJOIN:"+hashjoin+"\n"+
    					   "MERGEJOIN:"+mergejoin+"\n"+
    					   "NESTEDLOOP:"+nestloop);
    }
    
    private static void executeQuery(String query) throws SQLException{
    	long start = System.currentTimeMillis();
    	connection.createStatement().execute(query);
    	System.out.println("QUERY EXECUTED\nDURATION: "+(System.currentTimeMillis()-start));	
    }
}
