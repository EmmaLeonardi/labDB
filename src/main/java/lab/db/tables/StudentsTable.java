package lab.db.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lab.utils.Utils;
import lab.db.Table;
import lab.model.Student;

public final class StudentsTable implements Table<Student, Integer> {
	// Concatenazione con students è safe perchè hard coded
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
		final String query = "SELECT * FROM " + TABLE_NAME + " WHERE id=?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, id);
			final var result = statement.executeQuery();
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
	 * 
	 * @param resultSet a ResultSet from which the Student(s) will be extracted
	 * @return a List of all the students in the ResultSet
	 */
	private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
		// Create an empty list, then
		// Inside a loop you should:
		// 1. Call resultSet.next() to advance the pointer and check there are still
		// rows to fetch
		// 2. Use the getter methods to get the value of the columns
		// 3. After retrieving all the data create a Student object
		// 4. Put the student in the List
		// Then return the list with all the found students

		// Helpful resources:
		// https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
		// https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
		final List<Student> list = new ArrayList<>();
		if (resultSet != null) {
			try {
				while (resultSet.next()) {
					list.add(new Student(resultSet.getInt("id"), resultSet.getString("firstName"),
							resultSet.getString("lastName"), Optional.ofNullable(Utils.sqlDateToDate(resultSet.getDate("birthday")))));

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
		final String query = "SELECT * FROM " + TABLE_NAME;
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			final var result = statement.executeQuery();
			return readStudentsFromResultSet(result);
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return new ArrayList<>();
		}
	}

	public List<Student> findByBirthday(final Date date) {
		final String query = "SELECT * FROM " + TABLE_NAME + " WHERE birthday=?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setDate(1, Utils.dateToSqlDate(date));
			final var result = statement.executeQuery();
			return readStudentsFromResultSet(result);
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return new ArrayList<>();
		}
	}

	@Override
	public boolean dropTable() {
		final String query = "DROP TABLE " + TABLE_NAME;
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.executeUpdate(query);
			return true;
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return false;
		}
		
	}

	@Override
	public boolean save(final Student student) {
		final String query = "INSERT INTO " + TABLE_NAME + " VALUES (?,?,?,?)";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, student.getId());
			statement.setString(2, student.getFirstName());
			statement.setString(3, student.getLastName());
			statement.setDate(4, Utils.dateToSqlDate(student.getBirthday().orElse(null)));
			final var r=statement.executeUpdate();
			return r==1;
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return false;
		}
	}

	@Override
	public boolean delete(final Integer id) {
		final String query = "DELETE FROM " + TABLE_NAME + " WHERE id=?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, id);
			final var r = statement.executeUpdate();
			return r==1;
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return false;
		}
	}

	@Override
	public boolean update(final Student student) {
		final String query = "UPDATE " + TABLE_NAME + " SET id=?, firstName=?, lastName=?, birthday=? WHERE id=?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, student.getId());
			statement.setString(2, student.getFirstName());
			statement.setString(3, student.getLastName());
			statement.setDate(4, Utils.dateToSqlDate(student.getBirthday().orElse(null)));
			statement.setInt(5, student.getId());
			final var r = statement.executeUpdate();
			return r==1;
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return false;
		}
	}
}