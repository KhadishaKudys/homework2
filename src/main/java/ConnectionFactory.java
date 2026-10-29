import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ConnectionFactory {

    final static String POSTGRESQL = "jdbc:postgresql://localhost:5432/postgres";
    final static String USERNAME = System.getenv("username");
    final static String PASSWORD = System.getenv("password");

    public static Connection getDefaultParameterConnection(final String JDBCURL) {
        try {
            return DriverManager.getConnection(JDBCURL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}


