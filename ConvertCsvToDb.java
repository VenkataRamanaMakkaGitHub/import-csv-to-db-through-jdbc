package practice;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class ConvertCsvToDb {
	private static final String CONNECTION_STRING = "jdbc:sqlserver://localhost:1433;user=SA;password=<YourStrong@Passw0rd>;database=task;encrypt=true;trustServerCertificate=true";

	public static void main(String[] args) throws IOException {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = DriverManager.getConnection(CONNECTION_STRING);

			statement = connection.createStatement();
			statement.executeUpdate(
					"CREATE TABLE Address (id INT PRIMARY KEY, city VARCHAR(255), zip_code VARCHAR(10), zip_code_3d VARCHAR(10), postal_code VARCHAR(10), state_code VARCHAR(10), city_alias VARCHAR(255), location VARCHAR(255))");
			statement.executeUpdate(
					"CREATE TABLE Miles (id INT PRIMARY KEY, origin VARCHAR(10), destination VARCHAR(10), miles FLOAT)");

			String addressCsvFile = "/home/venkataramana/Downloads/Address.txt";
			BufferedReader addressReader = new BufferedReader(new FileReader(addressCsvFile));
			String addressLine;
			PreparedStatement addressInsertStatement = connection.prepareStatement(
					"INSERT INTO Address (id, city, zip_code, zip_code_3d, postal_code, state_code, city_alias, location) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
			addressReader.readLine(); // Skip the header line
			int count = 1;
			while ((addressLine = addressReader.readLine()) != null) {
				String[] addressData = addressLine.split(",");
				addressInsertStatement.setInt(1, Integer.parseInt(addressData[0]));
				count++;
				System.out.println(count);
				addressInsertStatement.setString(2, addressData[1]);
				addressInsertStatement.setString(3, addressData[2]);
				addressInsertStatement.setString(4, addressData[3]);
				addressInsertStatement.setString(5, addressData[4]);
				addressInsertStatement.setString(6, addressData[5]);
				addressInsertStatement.setString(7, addressData[6]);
				addressInsertStatement.setString(8, addressData[7]);
				addressInsertStatement.executeUpdate();
			}
			addressReader.close();
			
			String milesCsvFile = "/home/venkataramana/Downloads/miles.txt";
			BufferedReader milesReader = new BufferedReader(new FileReader(milesCsvFile));
			String milesLine;

			milesReader.readLine(); 
			PreparedStatement milesInsertStatement = connection
					.prepareStatement("INSERT INTO Miles (id, origin, destination, miles) VALUES (?, ?, ?, ?)");
			while ((milesLine = milesReader.readLine()) != null) {
				String[] milesData = milesLine.split(",");
				milesInsertStatement.setInt(1, Integer.parseInt(milesData[0]));
				milesInsertStatement.setString(2, milesData[1]);
				milesInsertStatement.setString(3, milesData[2]);
				milesInsertStatement.setFloat(4, Float.parseFloat(milesData[3]));
				milesInsertStatement.executeUpdate();
			}
			milesReader.close();
			System.out.println("Data inserted successfully into database.");
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
