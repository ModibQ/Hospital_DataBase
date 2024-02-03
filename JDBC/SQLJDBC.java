import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLJDBC {

    public static void main(String[] args) {
        // Establish connection
        String databaseUrl = "jdbc:sqlite:final.db";
        String driverClass = "org.sqlite.JDBC";

        try {
            Class.forName(driverClass);
            Connection connection = DriverManager.getConnection(databaseUrl);
            Statement statement = connection.createStatement();

            // Execute queries
            query1(statement);
            query2(statement);
            query3(statement);
            query4(statement);
            query5(statement);
            query6(statement);
            query7(statement);
            query8(statement);
            query9(connection);
            query10(connection);

            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void query1(Statement statement) throws SQLException {
        System.err.println("\nQuery 1:\n");
        String query = "SELECT D.D_NAME FROM DOCTORS D "
        + "JOIN P_ASSIGNMENT PA ON D.D_ID = PA.D_ID "
        + "JOIN PATIENTS P ON PA.P_ID = P.P_ID "
        + "WHERE P.P_NAME = 'RICHARD MILLER'";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Doctors consulting RICHARD MILLER:\n");
        while (resultSet.next()) {
            System.out.println(resultSet.getString("D_NAME"));
        }
        resultSet.close();
    }

    private static void query2(Statement statement) throws SQLException {
        System.err.println("\nQuery 2:\n");
        String query = "SELECT P.P_NAME, P.P_ID, P.P_DISEASE, T.T_NAME, T.T_RESULT "
        + "FROM TESTS T "
        + "JOIN PATIENTS P ON T.P_ID = SUBSTR(P.P_ID, -4) "
        + "WHERE P.P_DISEASE LIKE '%cancer%'";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Test results of cancer patients:\n");
        while (resultSet.next()) {
            System.out.println("Name: " + resultSet.getString("P_NAME"));
            System.out.println("Patient ID: " + resultSet.getString("P_ID"));
            System.out.println("Disease: " + resultSet.getString("P_DISEASE"));
            System.out.println("Test Name: " + resultSet.getString("T_NAME"));
            System.out.println("Test Result: " + resultSet.getString("T_RESULT"));
            System.out.println();
        }
        resultSet.close();
    }

    private static void query3(Statement statement) throws SQLException {
        System.err.println("\nQuery 3:\n");
        String query = "SELECT * FROM INSTRUMENTS WHERE I_MANUFACTURER LIKE 'S%'";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Instruments produced by manufacturers starting with 'S':\n");
        while (resultSet.next()) {
            System.out.println("Instrument ID: " + resultSet.getString("I_ID"));
            System.out.println("Instrument Name: " + resultSet.getString("I_NAME"));
            System.out.println("Manufacturer: " + resultSet.getString("I_MANUFACTURER"));
            System.out.println();
        }
        resultSet.close();
    }

    private static void query4(Statement statement) throws SQLException {
        System.err.println("\nQuery 4:\n");
        String query = "SELECT D_NAME, D_YEARS_OF_EXPERIENCE FROM DOCTORS "
        + "ORDER BY CAST(D_YEARS_OF_EXPERIENCE AS INTEGER) DESC LIMIT 1";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Most experienced doctor in the hospital:\n");
        while (resultSet.next()) {
            System.out.println("Doctor Name: " + resultSet.getString("D_NAME"));
            System.out.println("Years of Experience: " + resultSet.getString("D_YEARS_OF_EXPERIENCE"));
        }
        resultSet.close();
    }

    private static void query5(Statement statement) throws SQLException {
        System.err.println("\nQuery 5:\n");
        String query = "SELECT P.P_NAME, P.P_STREET, P.P_CITY "
        + "FROM PATIENTS P "
        + "JOIN P_ASSIGNMENT PA ON P.P_ID = PA.P_ID "
        + "JOIN DOCTORS D ON PA.D_ID = D.D_ID "
        + "WHERE D.D_NAME = 'JAMES SMITH' AND P.P_STREET = D.D_STREET AND P.P_CITY = D.D_CITY";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Patients of doctor JAMES SMITH living in the same street and city:\n");
        while (resultSet.next()) {
            System.out.println("Patient Name: " + resultSet.getString("P_NAME"));
            System.out.println("Street: " + resultSet.getString("P_STREET"));
            System.out.println("City: " + resultSet.getString("P_CITY"));
            System.out.println();
        }
        resultSet.close();
    }

    private static void query6(Statement statement) throws SQLException {
        System.err.println("\nQuery 6:\n");
        String query = "SELECT N.N_NAME, COUNT(DISTINCT NA.D_ID) AS Doctors_Assisted "
        + "FROM NURSES N "
        + "JOIN N_ASSISTS NA ON N.N_ID = NA.N_ID "
        + "GROUP BY N.N_NAME "
        + "HAVING COUNT(DISTINCT NA.D_ID) >= 2";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Nurses who assist at least two doctors:\n");
        while (resultSet.next()) {
            System.out.println("Nurse Name: " + resultSet.getString("N_NAME"));
            System.out.println("Number of Doctors Assisted: " + resultSet.getString("Doctors_Assisted"));
            System.out.println();
        }
        resultSet.close();
    }

    private static void query7(Statement statement) throws SQLException {
        System.err.println("\nQuery 7:\n");
        String query = "SELECT D.D_NAME, COUNT(DISTINCT NA.N_ID) AS Nurses_Count "
        + "FROM DOCTORS D "
        + "JOIN N_ASSISTS NA ON D.D_ID = NA.D_ID "
        + "GROUP BY D.D_NAME "
        + "ORDER BY Nurses_Count DESC";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Doctors and the number of nurses they have in descending order:\n");
        while (resultSet.next()) {
            System.out.println("Doctor Name: " + resultSet.getString("D_NAME"));
            System.out.println("Number of Nurses: " + resultSet.getString("Nurses_Count"));
            System.out.println();
        }
        resultSet.close();
    }

    private static void query8(Statement statement) throws SQLException {
        System.err.println("\nQuery 8:\n");
        String query = "SELECT N_NAME "
        + "FROM NURSES "
        + "WHERE N_ID NOT IN (SELECT DISTINCT N_ID FROM N_ASSISTS)";
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Nurses not assigned to any doctors:\n");
        while (resultSet.next()) {
            System.out.println("Nurse Name: " + resultSet.getString("N_NAME"));
        }
        resultSet.close();
    }
    
    private static void query9(Connection connection) throws SQLException {
        System.err.println("\nQuery 9:\n");
        String updateQuery = "UPDATE DOCTORS SET D_YEARS_OF_EXPERIENCE = D_YEARS_OF_EXPERIENCE + 5 WHERE D_GENDER = 'f'";
        try (Statement statement = connection.createStatement()) {
            int rowsUpdated = statement.executeUpdate(updateQuery);
            System.out.println("Years of experience updated for " + rowsUpdated + " female doctors.");
        }
    }

    private static void query10(Connection connection) throws SQLException {
        System.err.println("\nQuery 10:\n");
        String deleteQuery = "DELETE FROM TESTS WHERE T_RESULT = 'Negative'";
        try (Statement statement = connection.createStatement()) {
            int rowsDeleted = statement.executeUpdate(deleteQuery);
            if(rowsDeleted == 0){
                System.out.println("All tests deleted no more Negative Tests.\n");
                return;
            }
            System.out.println("Deleted " + rowsDeleted + " tests with negative results.\n");
        }
    }

}