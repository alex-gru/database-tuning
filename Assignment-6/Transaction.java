/** 
 * Dummy transaction that prints a start message, waits for a random time 
 * (up to 100ms) and finally prints a status message at termination.
 */

import java.sql.*;
public class Transaction extends Thread {

	// identifier of the transaction
	int i;
	Connection con;
	
	Transaction(int i, Connection con) {
		this.i = i;
		this.con = con;
	}
	
	@Override
	public void run() {
		try{
		System.out.println("transaction " + i + " started");
		String command = "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;"+
						 "UPDATE \"Accounts\" SET balance =(SELECT balance FROM \"Accounts\" WHERE account="+i+")+1 WHERE account="+i+";"+
						 "UPDATE \"Accounts\" SET balance =(SELECT balance FROM \"Accounts\" WHERE account=0)-1 WHERE account=0;"+
						 "COMMIT;";
		con.createStatement().execute(command);
		System.out.println("transaction " + i + " finished.");
		}catch(Exception e){
			e.printStackTrace();
		}
	}		
}
