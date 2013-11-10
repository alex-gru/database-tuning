import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;

public class Main {
    static Connection con;
    static BaseConnection baseCon;
    static CopyManager manager;

    /*
    Don't forget to store the postgresql driver in the working directory.
    */

    public static void main(String[] args) throws SQLException {
        try {
            con = DBConnector.getConnection();
            baseCon = (BaseConnection) con;
            manager = new CopyManager(baseCon);           
            select();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void select() throws SQLException, IOException {
//    	DISTINCT
//    	String command = "EXPLAIN ANALYZE SELECT DISTINCT ssnum FROM Employee WHERE dept = 'techdept10'";
//    	String command = "EXPLAIN ANALYZE SELECT ssnum FROM Employee WHERE dept = 'techdept10'";
    	
//    	NON-CORRELATED SUBQUERIES
//    	String command = "EXPLAIN ANALYZE SELECT ssnum FROM Employee WHERE dept IN (SELECT dept FROM Techdept)";
//    	String command = "EXPLAIN ANALYZE SELECT ssnum FROM Employee, Techdept WHERE Employee.dept = Techdept.dept";
    	
//    	TEMP TABLE
//still exists	 String command = "SELECT * INTO Temp FROM Employee	WHERE salary > 40000";   	
    	String command = "EXPLAIN ANALYZE SELECT ssnum FROM Temp WHERE Temp.dept = 'techdept10'";
//    	String command = "EXPLAIN ANALYZE SELECT ssnum FROM Employee WHERE Employee.dept = 'techdept10' AND salary > 40000";
    	
//    	JOIN, clustering index and numeric values
//    	String command = "EXPLAIN ANALYZE SELECT Employee.ssnum FROM Employee, Student WHERE Employee.name = Student.name";
//    	String command = "EXPLAIN ANALYZE SELECT Employee.ssnum FROM Employee, Student WHERE Employee.ssnum = Student.ssnum";
    	  	
//    	HAVING
//    	String command = "EXPLAIN ANALYZE SELECT AVG(salary) as avgsalary, dept	FROM Employee GROUP BY dept	HAVING dept = 'techdept10'";
//    	String command = "EXPLAIN ANALYZE SELECT AVG(salary) as avgsalary, dept FROM Employee WHERE dept = 'techdept10' GROUP BY dept";
    	
//    	VIEW
//still exists	String command = "CREATE VIEW Techlocation AS SELECT ssnum, Techdept.dept, location	FROM Employee, Techdept	WHERE Employee.dept = Techdept.dept;";
//    	String command ="EXPLAIN ANALYZE SELECT dept FROM Techlocation WHERE ssnum = 47086";
//    	String command ="EXPLAIN ANALYZE SELECT dept FROM Employee WHERE ssnum = 47086";
  
//    	NO INDIZES with OR
//    	String command = "EXPLAIN ANALYZE SELECT Employee.ssnum FROM Employee WHERE Employee.name = 'name1' OR Employee.dept = 'dept899'";
//    	String command = "EXPLAIN ANALYZE SELECT Employee.ssnum FROM Employee WHERE Employee.name = 'name1' UNION SELECT Employee.ssnum FROM Employee WHERE Employee.dept = 'dept899'";
    	
    	ResultSet rs = con.createStatement().executeQuery(command);
       
        System.out.println("Result:");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}