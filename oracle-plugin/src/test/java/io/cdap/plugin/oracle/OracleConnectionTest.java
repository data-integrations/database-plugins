import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class OracleConnectionTest {

    public static void main(String[] args) {
        String host = System.getProperty("oracle.host", "oracle");
        String port = System.getProperty("oracle.port", "1521");
        String username = System.getProperty("oracle.username", "SYSTEM");
        String password = System.getProperty("oracle.password", "oracle");
        String database = System.getProperty("oracle.database", "FREEPDB1");
        String connectionType = System.getProperty("oracle.connectionType", "service");

        String url;
        if ("service".equalsIgnoreCase(connectionType)) {
            url = String.format("jdbc:oracle:thin:@//%s:%s/%s", host, port, database);
        } else {
            url = String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, database);
        }

        System.out.println("Attempting to connect to: " + url);

        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        try {
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("Oracle JDBC Driver Registered!");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your Oracle JDBC Driver? Include in your library path!");
            e.printStackTrace();
            return;
        }

        try (Connection connection = DriverManager.getConnection(url, props)) {
            if (connection != null) {
                System.out.println("Successfully connected to Oracle!");
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM DUAL")) {
                    if (rs.next()) {
                        System.out.println("SELECT COUNT(*) FROM DUAL Result: " + rs.getInt(1));
                    }
                }
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT status FROM v$instance")) {
                    if (rs.next()) {
                        System.out.println("Instance Status: " + rs.getString(1));
                    }
                }
            } else {
                System.out.println("Failed to make connection!");
            }
        } catch (Exception e) {
            System.out.println("Connection Failed!");
            e.printStackTrace();
        }
    }
}
