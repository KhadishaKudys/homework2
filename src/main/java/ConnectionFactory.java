import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ConnectionFactory {

    final static String POSTGRESQL = "jdbc:postgresql://localhost:5432/postgres";


    static Connection getDefaultParameterConnection(final String databseURL) {
        try {
            return DriverManager.getConnection(databseURL, "postgres", "password");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}


