package lab.db.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lab.utils.Utils;
import lab.db.Table;
import lab.model.Student;

public final class StudentsTable implements Table<Student, Integer> {
	// Concatenazione con students � safe perch� hard coded
	public static final String TABLE_NAME = "students";

	private final Connection connection;

	public StudentsTable(final Connection connection) {
		this.connection = Objects.requireNonNull(connection);
	}

	@Override
	public String getTableName() {
		return TABLE_NAME;
	}

	@Override
	public boolean createTable() {
		// 1. Create the statement from the open connection inside a try-with-resources
		try (final Statement statement = this.connection.createStatement()) {
			// 2. Execute the statement with the given query
			statement.executeUpdate("CREATE TABLE " + TABLE_NAME + " (" + "id INT NOT NULL PRIMARY KEY,"
					+ "firstName CHAR(40)," + "lastName CHAR(40)," + "birthday DATE" + ")");
			return true;
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return false;
		}
	}

	@Override
	public Optional<Student> findByPrimaryKey(final Integer id) {
		final String query = "SELECT * FROM " + TABLE_NAME + " WHERE ID=?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, id);
			final var result = statement.executeQuery(query);
			/*
			 * try(final Statement statement = this.connection.createStatement()){ final var
			 * result=statement.executeQuery("SELECT * FROM "+TABLE_NAME+" WHERE ID="+id);
			 * //SQL injection :(, usa prepared statement
			 */

			return readStudentsFromResultSet(result).stream().findFirst();
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return Optional.empty();
		}
	}

	/**
      * Given a ResultSet read all the students in it and collects them in a List
      * @param resultSet a ResultSet from which the Student(s) will be extracted
      * @return a List of all the students in the ResultSet
      */
     private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
         // Create an empty list, then
         // Inside a loop you should:
         //      1. Call resultSet.next() to advance the pointer and check there are still rows to fetch
         //      2. Use the getter methods to get the value of the columns
         //      3. After retrieving all the data create a Student object
         //      4. Put the student in the List
         // Then return the list with all the found students

         // Helpful resources:
         // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
         // https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
    	 final List<Student> list=new ArrayList<>();
    	 if(resultSet!=null) {
    	 try {
			while(resultSet.next()) {
				 list.add(new Student(resultSet.getInt("id"), resultSet.getString("firstName"), resultSet.getString("lastName"), Optional.of(resultSet.getDate("birthday"))));
				
			 }
		} catch (SQLException e) {
			// Handle possibile sql exception
			e.printStackTrace();
		}
    	 }
         return list;
     }

	@Override
	public List<Student> findAll() {
		throw new UnsupportedOperationException("TODO");
	}

	public List<Student> findByBirthday(final Date date) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public boolean dropTable() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public boolean save(final Student student) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public boolean delete(final Integer id) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public boolean update(final Student student) {
		throw new UnsupportedOperationException("TODO");
	}
}