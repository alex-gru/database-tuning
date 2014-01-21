/*
 * Example code for Assignment 6 (concurrency tuning) of the course:
 * 
 * Database Tuning
 * Department of Computer Science
 * University of Salzburg, Austria
 * 
 * Lecturer: Nikolaus Augsten
 */

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * <p/>
 * Run numThreads transactions, where at most maxConcurrent transactions can run
 * in parallel.
 */
public class ConcurrentTransactions {

    static Connection con;
    static FileWriter out;

    public static void main(String[] args) throws IOException {
        out = new FileWriter(new File("results_task1.txt"));
        out.write("");
        runTask(1);
        out.close();
        out = new FileWriter(new File("results_task2.txt"));
        out.write("");
        runTask(2);
        out.close();
    }

    private static void runTask(int task) {
        try {
            int isolation = Connection.TRANSACTION_READ_COMMITTED;
            for (int maxConcurrent = 1; maxConcurrent <= 5; maxConcurrent++) {
                con = new DBConnector().getConnection();
                resetDB(con);
                runTest(maxConcurrent, task, isolation);
            }

            isolation = Connection.TRANSACTION_SERIALIZABLE;
            for (int maxConcurrent = 1; maxConcurrent <= 5; maxConcurrent++) {
                con = new DBConnector().getConnection();
                resetDB(con);
                runTest(maxConcurrent, task, isolation);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void resetDB(Connection con) throws SQLException {
        con.createStatement().execute("update \"Accounts\" set balance=0 where account != 0;\n" +
                "update \"Accounts\" set balance=100 where account = 0");
    }

    private static void runTest(Connection con, int maxConcurrent, int task) throws Exception {
        System.out.println("----------------------\nTASK " + task);
        if (con.getTransactionIsolation() == 8) {
            System.out.println("SERIALIZABLE");
        } else if (con.getTransactionIsolation() == 2) {
            System.out.println("READ COMMITTED");
        } else {
            throw new Exception("????");
        }

        System.out.println("Max Concurrent Threads: " + maxConcurrent);
        // create numThreads transactions
        Transaction[] trans = new Transaction[100];
        long start = System.currentTimeMillis();

        for (int i = 0; i < trans.length; i++) {
//            trans[i] = new Transaction(i + 1, con, task);
        }
        // start all transactions using a thread pool
        ExecutorService pool = Executors.newFixedThreadPool(maxConcurrent);

        for (int i = 0; i < trans.length; i++) {
            pool.execute(trans[i]);
        }

        pool.shutdown(); // end program after all transactions are done
        pool.awaitTermination(30, TimeUnit.SECONDS);
        float time = (float) (System.currentTimeMillis() - start) / 1000;
        float throughput = 100 / time;
        ResultSet rs = con.createStatement().executeQuery("select balance from \"Accounts\" " +
                " where account=0");
        rs.next();

        float correctness = (float) (100 - rs.getInt(1)) / 100;
        System.out.println("DURATION: " + time + " seconds");
        System.out.println("Throughput: " + throughput + " trans/second");
        System.out.println("Correctness: " + correctness);
    }

    private static void runTest(int maxConcurrent, int task, int isolation) throws Exception {
        out.append("----------------------\nTASK " + task);
        out.append("\n");

        if (isolation == 8) {
            out.append("SERIALIZABLE");
        } else if (isolation == 2) {
            out.append("READ COMMITTED");
        } else {
            throw new Exception("????");
        }
        out.append("\n");
        out.append("Max Concurrent Threads: " + maxConcurrent);
        out.append("\n");

        // create numThreads transactions
        Transaction[] trans = new Transaction[100];
        long start = System.currentTimeMillis();

        for (int i = 0; i < trans.length; i++) {
//            trans[i] = new Transaction(i + 1, con, task);
            trans[i] = new Transaction(i + 1, task, isolation);
        }
        // start all transactions using a thread pool
        ExecutorService pool = Executors.newFixedThreadPool(maxConcurrent);

        for (int i = 0; i < trans.length; i++) {
            pool.execute(trans[i]);
        }

        pool.shutdown(); // end program after all transactions are done
        pool.awaitTermination(30, TimeUnit.SECONDS);
        float time = (float) (System.currentTimeMillis() - start) / 1000;
        float throughput = 100 / time;
        ResultSet rs = con.createStatement().executeQuery("select balance from \"Accounts\" " +
                " where account=0");
        rs.next();

        float correctness = (float) (100 - rs.getInt(1)) / 100;
        out.append("DURATION: " + time + " seconds");
        out.append("\n");
        out.append("Throughput: " + throughput + " trans/second");
        out.append("\n");
        out.append("Correctness: " + correctness);
        out.append("\n");

    }
}
