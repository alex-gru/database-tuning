/**
 * Dummy transaction that prints a start message, waits for a random time 
 * (up to 100ms) and finally prints a status message at termination.
 */


import java.sql.*;

public class Transaction extends Thread {

    private final int task;
    private final int isolation;
    // identifier of the transaction
    Connection con;
    int i;

    Transaction(int i, Connection con, int task, int isolation) {
        this.i = i;
        this.con = con;
        this.task = task;
        this.isolation = isolation;
    }

    Transaction(int i, int task, int isolation) {
        this.i = i;
        this.task = task;
        this.isolation = isolation;
    }

    @Override
    public void run() {
        boolean rolledBack = true;

//            System.out.println("transaction " + i + " started");

        try {
            con = new DBConnector().getConnection();
            con.setAutoCommit(false);
            con.setTransactionIsolation(isolation);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (task == 1) {
            while (rolledBack) {
                try {
                    runTask1();
                    rolledBack = false;
                } catch (SQLException e) {
//                    e.printStackTrace();
                    rolledBack = true;
                    try {
                        con.close();
                        con = new DBConnector().getConnection();
                        con.setAutoCommit(false);
                        con.setTransactionIsolation(isolation);
                    } catch (Exception e1) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            while (rolledBack) {
                try {
                    runTask2();
                    rolledBack = false;
                } catch (SQLException e) {
//                    e.printStackTrace();
                    rolledBack = true;
                    try {
                        con.close();
                        con = new DBConnector().getConnection();
                        con.setAutoCommit(false);
                        con.setTransactionIsolation(isolation);
                    } catch (Exception e1) {
                        e.printStackTrace();
                    }
                }
            }
        }
//            System.out.println("transaction " + i + " finished.");
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void runTask1() throws SQLException {
        String command = "SELECT balance FROM \"Accounts\" WHERE account=" + i;
//            System.out.println(command);
        ResultSet rs = con.createStatement().executeQuery(command);
        rs.next();
        int bal = rs.getInt(1);

        command = "UPDATE \"Accounts\" SET balance =" + (bal + 1) + " WHERE account=" + i;
//            System.out.println(command);
        con.createStatement().execute(command);

        command = "SELECT balance FROM \"Accounts\" WHERE account=0";
//            System.out.println(command);
        rs = con.createStatement().executeQuery(command);
        rs.next();
        int balBank = rs.getInt(1);
//        System.out.println("balance Bank: " + balBank);
        command = "UPDATE \"Accounts\" SET balance =" + (balBank - 1) + " WHERE account=0";
//            System.out.println(command);
        con.createStatement().execute(command);
        con.commit();
    }

    private void runTask2() throws SQLException {
        String command = "UPDATE \"Accounts\" SET balance = balance + 1 WHERE account=" + i;
//            System.out.println(command);
        con.createStatement().execute(command);
        command = "UPDATE \"Accounts\" SET balance = balance - 1 WHERE account=0";
//            System.out.println(command);
        con.createStatement().execute(command);
        con.commit();
    }
}
