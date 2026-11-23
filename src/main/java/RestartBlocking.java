import enums.OperationType;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class RestartBlocking {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;
    private static final long BASE_BACKOFF_MS = 0;

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;

    public static final int NUMBER_OF_THREADS = 1;
    private static final AtomicInteger SUM_INDIVIDUAL_TUPLES = new AtomicInteger();
    private static final AtomicInteger INSERT = new AtomicInteger();
    private static final AtomicInteger SUM_WHOLE_TABLE_SQL = new AtomicInteger();
    private static final AtomicInteger UPDATE_3_ENTRIES = new AtomicInteger();
    private static final AtomicInteger tasks_finished = new AtomicInteger();

    void executeSQL(String sql, Connection con) {
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing " + sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void cerateDB() {
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS RestartBlocking_data;", con);
            executeSQL("CREATE TABLE RestartBlocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO RestartBlocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
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

    public static void main(String[] args) throws InterruptedException {
        //Create DB
        RestartBlocking b = new RestartBlocking();
        b.cerateDB();

        // Thread safe methods of storing running state and the current task. Essentially an ID.
        AtomicBoolean isRunning = new AtomicBoolean(true);
        AtomicInteger currentTaskNumber = new AtomicInteger(0);

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(NUMBER_OF_THREADS);//XXX - Do not change the number of threads used

        final var start = System.currentTimeMillis();

        // create a certain number of workers and each continues to create new transactions
        // until isRunning is false (about 30 seconds).
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            executor.execute(() -> {
                while (isRunning.get()) {
                    int task = currentTaskNumber.getAndIncrement();
                    b.new Blocker(task).run();
                }
            });
        }

        // Run benchmark for 30 seconds
        Thread.sleep(30_000);

        isRunning.set(false);
        executor.shutdown();

        // we wait for the current transactions to complete
        while (!executor.isTerminated()) {
            Thread.sleep(100);
        }

        double stop = System.currentTimeMillis();
        System.out.println("[DONE] Finished " + tasks_finished + " transactions in " + (stop - start) / 1000.0 + "s");
        System.out.println("Operation distribution:");
        System.out.println("Sum individual tuples: " + SUM_INDIVIDUAL_TUPLES.get());
        System.out.println("Insert: " + INSERT.get());
        System.out.println("Sum whole table (SQL): " + SUM_WHOLE_TABLE_SQL.get());
        System.out.println("Update entries: " + UPDATE_3_ENTRIES.get());
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker implements Runnable {
        final int task;
        final double[] start_range;
        final double[] stop_range;
        final double p;

        private static final String READ_ONLY_QUERY = "SELECT data_value FROM RestartBlocking_data WHERE data_id = ?";
        private static final String INSERT_QUERY = "INSERT INTO RestartBlocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?) RETURNING data_id";
        private static final String SUM_WHOLE_TABLE = "SELECT SUM(data_value) AS sum FROM RestartBlocking_data";
        private static final String UPDATE_TUPLE = "UPDATE RestartBlocking_data " +
                "SET data_value = ?, modified_by = ? " +
                "WHERE data_id = (" +
                "    SELECT data_id FROM RestartBlocking_data " +
                "    WHERE data_value >= ? AND data_value <= ? AND data_id != ALL(?)" +
                "    LIMIT 1" +
                ") RETURNING data_id";

        public Blocker(int id) {
            this.task = id;

            System.out.println("TASK ID: " + id);
            this.start_range = new double[num_queries_per_task];
            this.stop_range = new double[num_queries_per_task];

            // Using ThreadLocalRandom as "The random number obtained
            // by one thread is not affected by the other thread
            // whereas java.util.Random provides random numbers globally"
            // From: https://www.baeldung.com/java-thread-local-random
            this.p = ThreadLocalRandom.current().nextDouble(0, 1);

            for (int q = 0; q < num_queries_per_task; q++) {
                stop_range[q] = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
            }
        }

        private StringBuilder SuccessfulCompletion(final String operation, final int attempt) {
            int doneCount = tasks_finished.incrementAndGet();

            return new StringBuilder()
                    .append("Done task ")
                    .append(this.task)
                    .append(" after ")
                    .append(attempt)
                    .append(" attempt(s). I am finisher ")
                    .append(doneCount)
                    .append("\n")
                    .append(operation)
                    .append("\n");
        }

        private void readMultipleTuples(final Connection connection, final int attempt) {
            // generate infinite stream of ints via lambda
            final var results = IntStream.generate(
                            () -> ThreadLocalRandom.current().nextInt(0, num_tuple)
                    )
                    // no duplicates & limit to global var
                    .distinct()
                    .limit(num_queries_per_task)
                    .mapToObj(idToRead -> {
                        try (PreparedStatement statement = connection.prepareStatement(READ_ONLY_QUERY)) {
                            statement.setInt(1, idToRead);
                            try (ResultSet rs = statement.executeQuery()) {
                                if (!rs.next()) {
                                    throw new SQLException("ERROR: No result found for id: " + idToRead);
                                }
                                return Pair.of(idToRead, rs.getDouble("data_value"));
                            }
                        } catch (SQLException error) {
                            throw new RuntimeException("Task " + this.task + " failed", error);
                        }
                    })
                    .toList();

            final var sum = results.stream()
                    .mapToDouble(Pair::getValue)
                    .sum();

            final var output = SuccessfulCompletion("Sum " + num_queries_per_task + " Tuples:", attempt);
            results.forEach(result ->
                    output.append(" id: ").append(result.getLeft())
                            .append(", value: ").append(result.getRight()).append("\n")
            );
            output.append("Sum: ").append(sum).append("\n");

            System.out.println(output);
            SUM_INDIVIDUAL_TUPLES.incrementAndGet();
        }

        private void insertEntry(final Connection connection, final int attempt) throws SQLException {
            try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
                statement.setInt(1, num_tuple + this.task);
                statement.setDouble(2, ThreadLocalRandom.current().nextDouble(0, 100));
                statement.setInt(3, ThreadLocalRandom.current().nextInt(1, 100));

                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Insert returned no data!");
                    }
                    final var id = rs.getInt("data_id");

                    System.out.println(SuccessfulCompletion("Inserted tuple: ID = " + id, attempt));
                    INSERT.incrementAndGet();
                }
            }
        }


        private void readWholeTable(Connection connection, final int attempt) throws SQLException {
            try (PreparedStatement statement = connection.prepareStatement(SUM_WHOLE_TABLE);
                 ResultSet rs = statement.executeQuery()) {

                if (!rs.next()) {
                    throw new SQLException("Full table SUM returned no result!");
                }

                double sum = rs.getDouble("sum");
                System.out.println(SuccessfulCompletion("Sum whole table: " + sum, attempt));
                SUM_WHOLE_TABLE_SQL.incrementAndGet();
            }
        }

        private void updateInRanges(Connection connection, final int attempt) throws SQLException {
            List<Integer> updatedIds = new ArrayList<>();

            int iterations = 0;
            // cap the number of iterations to avoid infinite loops
            while (updatedIds.size() < 3 && iterations < 10) {
                Array sqlArray = connection.createArrayOf(
                        "INTEGER",
                        updatedIds.toArray(Integer[]::new)
                );
                try (PreparedStatement statement = connection.prepareStatement(UPDATE_TUPLE)) {

                    statement.setDouble(1, rand.nextDouble() * max_value);

                    int q = ThreadLocalRandom.current()
                            .nextInt(num_queries_per_task);

                    statement.setInt(2, task);
                    statement.setDouble(3, start_range[q]);
                    statement.setDouble(4, stop_range[q]);

                    statement.setArray(5, sqlArray);

                    try (ResultSet rs = statement.executeQuery()) {
                        if (rs.next()) {
                            updatedIds.add(rs.getInt("data_id"));
                        }
                    }
                } finally {
                    sqlArray.free();
                }

                iterations++;
            }

            System.out.println(SuccessfulCompletion("Update entries: " + updatedIds, attempt));
            UPDATE_3_ENTRIES.incrementAndGet();
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
                try {
                    con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                    con.setAutoCommit(false);
                    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                    switch (OperationType.fromProbability(this.p)) {
                        case READ_MULTIPLE_TUPLES -> readMultipleTuples(con, attempt);
                        case INSERT_ENTRY -> insertEntry(con,attempt);
                        case READ_WHOLE_TABLE -> readWholeTable(con,attempt);
                        case UPDATE_IN_RANGES -> updateInRanges(con,attempt);
                        default -> throw new RuntimeException("Invalid range case detected!: " + p);
                    }

                    con.commit();
                    return;
                } catch (SQLException e) {
                    try {
                        con.rollback();
                    } catch (SQLException ignored) {
                    }
                    if (isRetryable(e)) {
                        try {
                            Thread.sleep(BASE_BACKOFF_MS * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    } else {
                        System.err.println("Task " + task + " failed: " + e.getMessage());
                        return;
                    }
                } finally {
                    try {
                        if (con != null) {
                            con.close();
                        }
                    } catch (SQLException ignored) {
                    }
                }
            }
        }
    }
}