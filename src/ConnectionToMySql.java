import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class ConnectionToMySql {

	private static String connectionUrl = "jdbc:mysql://localhost:3306/yelp?autoReconnect=true&useSSL=false";
	private static String connectionUser = "root";
	private static String connectionPassword = "nioosha7771";
	private static Connection conn;

	    public static Connection getConnection() {
	    		
 			 try {
 				Class.forName("com.mysql.jdbc.Driver");
 	           // Class.forName(driverName);
 	            try {
 	            	conn = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
 	            } catch (SQLException ex) {
 	                // log an exception. fro example:
 	                System.out.println("Failed to create the database connection."); 
 	            }
 	        } catch (ClassNotFoundException ex) {
 	            // log an exception. for example:
 	            System.out.println("Driver not found."); 
 	        }
 	        return conn;
	    }   
	
	
}
