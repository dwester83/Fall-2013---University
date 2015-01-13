import java.lang.System;

import java.util.Properties;

import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class University {
	
	Connection conn = null;
	Statement stmt  = null;
	PreparedStatement instructorPStmt = null;
	CallableStatement instructorCStmt = null;
	static final String instructorQuery = "SELECT * from instructor";
	static final String instructorCnt = "CALL Count_Courses(?,?)";

	/**
	 * University Main
	 */
	public static void main(String[] args) 
	{
		String userName = args[0];	// Input User Name for creating connection to University Schema
		String passWord = args[1];	// Input Password for creating connection to University Schema
	
		try
		{
			// Invoke the University constructor, passing in User Name and Password.
			University university = new University(userName, passWord);
		
			// Display the contents of the Instructor table,
			// first using a mere Statement object, with the query built in this method,
			// and then second, using a Prepared statement doing the same thing. 
			System.out.println("getInstructor ...");
			university.getInstructor();
			System.out.println("getPreparedInstructor ...");
			university.getPreparedInstructor();
			System.out.println("Count_Courses ...");
			university.countCourses();
			university.cleanup();
		}
		catch (SQLException exSQL)
		{
		    System.out.println("main SQLException: " + exSQL.getMessage());
		    System.out.println("main SQLState: " + exSQL.getSQLState());
		    System.out.println("main VendorError: " + exSQL.getErrorCode());
		    exSQL.printStackTrace();
		}
	}
	
	/**
	 * Constructor for Class University
	 * @param String userName for connecting to University schema
	 * @param String passWord for connecting to University schema
	 */
	public University(String userName, String passWord) throws SQLException
	{		
		// Go create a connection to my "university" database.
		// "conn" is a data member of JDBC type Connection.
		// See University::getConnection method below.
		conn = getConnection(userName, passWord);
		if (conn == null)
		{
			System.out.println("getConnection failed.");
			return;
		}
		
		// Create Statement object for use in creating the non-prepared queries.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/Connection.html
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/Statement.html
		stmt = conn.createStatement();
		
		// Create the Prepared statements.
		// A SQL statement is precompiled and stored in a PreparedStatement object.
		// This object can then be used to efficiently execute this statement multiple times,
		// rather than having it recompiled with each execution.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/PreparedStatement.html
		instructorPStmt = conn.prepareStatement(instructorQuery, ResultSet.FETCH_FORWARD);
		
		// Create a Callable statement to call a stored procedure returning a course count.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/CallableStatement.html
		instructorCStmt = conn.prepareCall(instructorCnt);
		
	}	// End of University Constructor.
	
	/**
	 * Method: cleanup()
	 * Function: To close the various JDBC objects.
	 */
	public void cleanup()
	{
		try {
			stmt.close();
			conn.close();
			instructorPStmt.close();
		}
		catch  (SQLException exSQL) {;}
	}
	
	public Connection getConnection(String userName, String passWord)
	{
		Connection conn = null;
		
		// Location of the MySQL-based "university" database.
		String university_url = "jdbc:mysql://localhost:3306/university";
			
		// Load the JDBC driver manager.
		// http://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
		// http://dev.mysql.com/doc/refman/5.5/en/connector-j-usagenotes-connect-drivermanager.html
		// The name of the class that implements java.sql.Driver in MySQL Connector/J is "com.mysql.jdbc.Driver".
		// The DriverManager needs to be told which JDBC driver with which it should try to make a Connection.
		// One way to do this is to use Class.forName() on the class that implements the java.sql.Driver interface.
		// With MySQL Connector/J, the name of this class is com.mysql.jdbc.Driver.
		try { Class.forName("com.mysql.jdbc.Driver").newInstance(); }
		catch (Exception ex) { 
		    System.out.println("Class.forName Exception: " + ex.getMessage());
		    ex.printStackTrace();
		    return null;
		}
		
		// Construct a Properties object for passing the User Name and Password into the DB connection.
		// Create the DB connection.
		// http://www.tutorialspoint.com/jdbc/jdbc-db-connections.htm
		try {
			Properties connectionProps = new Properties();
			connectionProps.put("user", userName);
			connectionProps.put("password", passWord);
			conn = DriverManager.getConnection (university_url, connectionProps);
			if (conn == null)
			{
				System.out.println("getConnection:getConnection failed.");
				return null;
			}
		}
		catch (SQLException exSQL)
		{
		    System.out.println("getConnection SQLException: " + exSQL.getMessage());
		    System.out.println("getConnection SQLState: " + exSQL.getSQLState());
		    System.out.println("getConnection VendorError: " + exSQL.getErrorCode());
		    exSQL.printStackTrace();
		}
		
		return conn;
	}	// End of getConnection()
	
	
	void getInstructor() throws SQLException
	{	
		// Define and execute the query used to see the contents of the Instructor table.
		// This query is "SELECT * from instructor".
		// With the completion of this method, the cursor is positioned before the first row.
		// Unlike the prepared statements, this needs to be compiled by the DB manager before execution.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/ResultSet.html
		ResultSet rs = stmt.executeQuery(instructorQuery);
		
		// In this example, we are going to work - using a cursor - backwards through the rows of the ResultSet.
		// Point the cursor after the last row in the result set.
		// The previous() method returns false where there are no more rows in the ResultSet.
		rs.afterLast();
		while(rs.previous())
		{
			// This query returns the following:
			// 1) ID		VARCHAR
			// 2) name		VARCHAR
			// 3) dept_name	VARCHAR
			// 4) salary	NUMERIC(8,2)
			String instructorID   = rs.getString("ID");
			String instructorName = rs.getString("name");
			String departmentName = rs.getString("dept_name");
			Float salary = rs.getFloat("salary");
			
			// ... and then print the results found at the current cursor location.
			System.out.format("Instructor name: %13s", instructorName);
			System.out.println("\t ID: " + instructorID + 
							   "\t Department: " + departmentName + 
							   "\t Salary: $" + salary);
		}	// End of while(rs.previous()) loop.
		
		rs.close();
	}	// End of getInstructor()
	
	void getPreparedInstructor() throws SQLException
	{
		// Go read all of the contents of the Instructor table per the prepared query.
		// With the completion of this method, the cursor is positioned before the first row.
		ResultSet rs = instructorPStmt.executeQuery();
	
		// Point the cursor at the next row in the result set.
		// The next() method returns false where there are no more rows in the ResultSet.
		while(rs.next())
		{
			// This query returns the following:
			// 1) ID		VARCHAR
			// 2) name		VARCHAR
			// 3) dept_name	VARCHAR
			// 4) salary	NUMERIC(8,2)
			String instructorID   = rs.getString(1);
			String instructorName = rs.getString(2);
			String departmentName = rs.getString(3);
			Float salary = rs.getFloat(4);
			
			// ... and then print the results found at the current cursor location.
			System.out.format("Instructor name: %13s", instructorName);
			System.out.println("\t ID: " + instructorID + 
							   "\t Department: " + departmentName + 
							   "\t Salary: $" + salary);
		}	// End of while(rs.next()) loop.
		
		rs.close();
	}	// End of getPreparedInstructor()
	
	void countCourses() throws SQLException
	{
		int course_count;
		String instructorName = "Brandt";		// Tell Call that we want to know the number of courses taught by "Brandt".
		instructorCStmt.setString(1, instructorName);
		instructorCStmt.registerOutParameter(2, Types.INTEGER);
		instructorCStmt.executeQuery();				// Execute the Call of the Stored Procedure Count_Courses.
		course_count = instructorCStmt.getInt(2);	// Glom onto the result, the count of the number of courses.
		System.out.println("Number of courses taught by " + instructorName + " is " + course_count + ".");
	}
	
}	// End of University Class
