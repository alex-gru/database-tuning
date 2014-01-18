/*
 * Example code for Assignment 6 (concurrency tuning) of the course:
 * 
 * Database Tuning
 * Department of Computer Science
 * University of Salzburg, Austria
 * 
 * Lecturer: Nikolaus Augsten
 */

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;

/**
 * <p>
 * Run numThreads transactions, where at most maxConcurrent transactions can run
 * in parallel.
 * 
 * <p>
 * params: numThreads maxConcurrent
 * 
 */
public class ConcurrentTransactions {

	static Connection con;

	public static void main(String[] args) {
		try {
			con = DBConnector.getConnection();
			// read command line parameters
			if (args.length != 2) {
				System.err.println("params: numThreads maxConcurrent");
				System.exit(-1);
			}
			int numThreads = Integer.parseInt(args[0]);
			int maxConcurrent = Integer.parseInt(args[1]);			
			// create numThreads transactions
			Transaction[] trans = new Transaction[numThreads];
			for (int i = 0; i < trans.length; i++) {
				trans[i] = new Transaction(i, con);
			}
			// start all transactions using a thread pool
			ExecutorService pool = Executors.newFixedThreadPool(maxConcurrent);
			for (int i = 0; i < trans.length; i++) {
				pool.execute(trans[i]);
			}
			pool.shutdown(); // end program after all transactions are done
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
