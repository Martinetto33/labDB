package lab.db.tables;

 import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.Statement;
 import java.sql.SQLException;
 import java.sql.SQLIntegrityConstraintViolationException;
 import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
 import java.util.List;
 import java.util.Objects;
 import java.util.Optional;

import com.google.protobuf.Option;

import lab.utils.Utils;
 import lab.db.Table;
 import lab.model.Student;

 public final class StudentsTable implements Table<Student, Integer> {    
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
             statement.executeUpdate(
                 "CREATE TABLE " + TABLE_NAME + " (" +
                         "id INT NOT NULL PRIMARY KEY," +
                         "firstName CHAR(40) NOT NULL," + 
                         "lastName CHAR(40), NOT NULL" + 
                         "birthday DATE" + 
                     ")");
             return true;
         } catch (final SQLException e) {
             // 3. Handle possible SQLExceptions
             return false;
         }
     }

     @Override
     public Optional<Student> findByPrimaryKey(final Integer id) {
        /* Statement is a resource; it means it has to be opened and closed, that's why
         * we use a try-with-resources.
         */
        try (final Statement statement = this.connection.createStatement()) {
            /* This implementation is very  unsafe and SHOULD NEVER BE USED!
             * The external parameter "id" in fact may contain malevolent code,
             * like a "DROP TABLE" query, that our database would execute to its death.
             * This kind of attacks are called "SQL injections". They can be stopped with
             * PreparedStatments.
             */
            final ResultSet resultSet = statement.executeQuery(
                "SELECT * FROM " + TABLE_NAME + " WHERE id = " + id 
                /* Ensure to leave spaces around words whent concatenating strings like this,
                 * otherwise there's the risk to create something that the SQL manager can't execute
                 * like 'STUDENTSWHERE id ='
                 */
            );
            return readStudentsFromResultSet(resultSet).stream().findFirst();
        } catch (final SQLException e) {
            return Optional.empty();
        }

        /*
         * TO AVOID SQL-INJECTIONS
         * 
         * final var query = "SELECT * FROM " + TABLE_NAME + " WHERE id = ?";
         * try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
         *      statement.setInt(1, id) // replaces the first occurrence of '?' in query with the integer value of id; this way no malevolent code stored in id's "toString" can be executed
         *      final ResultSet = statement.executeQuery();
         *      return readStudentsFromResultSet(resultSet).stream().findFirst();
         * } catch (final SQLException e) {
         *      return Optional.empty();
         * }
         */
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
         final List<Student> result = new ArrayList<>();
         try {
            while (resultSet.next()) {
                final var id = resultSet.getInt("id");
                final var firstName = resultSet.getString("firstName");
                final var lastName = resultSet.getString("lastName");
                final var date = Utils.sqlDateToDate(resultSet.getDate("birthday")); // this is needed because 'getDate()' returns a SQL.Date while we need a java.Date
                result.add(new Student(id, firstName, lastName, Optional.ofNullable(date)));
             }
        } catch (SQLException e) {
            e.printStackTrace();
        }
         return result;
     }

     @Override
     public List<Student> findAll() {
        final var query = "SELECT * FROM " + TABLE_NAME;
        try (final Statement statement = connection.createStatement()) {
            return this.readStudentsFromResultSet(statement.executeQuery(query));
        } catch (final SQLException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
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