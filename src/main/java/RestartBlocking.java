import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class RestartBlocking {
    public static int num_taks=30;
    public static int num_queries_per_task = 3;
    private static final long BASE_BACKOFF_MS = 20;
    private static final int MAX_RETRIES = 10;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;

    public RestartBlocking(){
        tasks_finished = new AtomicInteger(0);
    }

    void executeSQL(String sql, Connection con){
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing "+sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void cerateDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS RestartBlocking_data;",con);
            executeSQL("CREATE TABLE RestartBlocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO RestartBlocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i=0;i<num_tuple;i++){
                ps.setInt(1, i);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println(Arrays.toString(results));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        //Create DB
        RestartBlocking b = new RestartBlocking();
        b.cerateDB();

        //Create task and their queries
        ArrayList<Blocker> my_tasks = new ArrayList<RestartBlocking.Blocker>(num_taks);
        for(int task=0;task<num_taks;task++){
            my_tasks.add(b.new Blocker(task));
        }

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
        double start, stop;
        start = System.currentTimeMillis();
        for(Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        //wait for the tasks to finish
        while(!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks);
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker implements Runnable  {
        final int task;
        final double [] start_range;
        final double [] stop_range;

        public Blocker(int id){
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range  = new double[num_queries_per_task];
            for(int q=0;q<num_queries_per_task;q++) {
                stop_range[q]  = rand.nextDouble()*max_value;
                start_range[q] = rand.nextDouble()*stop_range[q];
            }
        }

        private boolean isRetryable(SQLException e) {
            String state = e.getSQLState();
            if (state == null) return false;
            // 40001 - Serialization failure, 40P01 - Deadlock detected, 55P03 - Lock not available
            return state.equals("40001") || state.equals("40P01") || state.equals(("55P03"));
        }

        @Override
        public void run() {
            int attempt = 0;
            while (true) {
                attempt++;
                Connection con = null;
                PreparedStatement ps = null;
                try {
                    con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                    con.setAutoCommit(false);
                    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    int q = 0;
                    ps = con.prepareStatement("UPDATE RestartBlocking_data SET data_value = ?, modified_by = " + task + " WHERE data_value >= ? AND data_value <= ?;", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    while (q < num_queries_per_task) {
                        ps.setDouble(1, (float) (rand.nextDouble() * max_value));
                        ps.setDouble(2, start_range[q]);
                        ps.setDouble(3, stop_range[q]);
                        ps.execute();
                        q++;
                    }
                    con.commit();
                    int num_finished = tasks_finished.incrementAndGet();
                    System.out.println("Done task " + task + " after " + attempt + " attempt(s). I am finisher number " + num_finished);
                    return;
                } catch (SQLException e) {
                    try {
                        con.rollback();
                    } catch (SQLException ignored) {}
                    if (isRetryable(e) && attempt < MAX_RETRIES) {
                        try {
                            Thread.sleep(BASE_BACKOFF_MS * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    else {
                        System.err.println("Task " + task + " failed: " + e.getMessage());
                        return;
                    }
                } finally {
                    try {
                        if (ps != null) {
                            ps.close();
                        }
                        if (con != null) {
                            con.close();
                        }
                    } catch (SQLException ignored) {}
                }
            }
        }
    }
}