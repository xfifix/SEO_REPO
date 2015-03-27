package cron.analytics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import com.google.common.base.Joiner;

public class DataBaseManagement {

	private static Connection connect;
	private static String selectCurrentRun = "select * from SERPOSCOPE.run where isnull(dateStop)";
	private static String runInsertString ="insert into SERPOSCOPE.run (dateStart, dateStop, logs, pid, haveError) VALUES (?,?,?,?,?)";
	private static String check_insert = "INSERT INTO SERPOSCOPE.check (idGroup,idRun,date) VALUES(?,?,?)";
	private static String keyword_insert = "INSERT INTO SERPOSCOPE.rank (idCheck,idTarget,idKeyword,position,url) VALUES (?,?,?,?,?)";
	private static String group_request = "select * from SERPOSCOPE.group";
	private static String keyword_query = "SELECT idKeyword,name FROM SERPOSCOPE.keyword WHERE idGroup=";
	private static String target_query = "SELECT idTarget,name FROM SERPOSCOPE.target WHERE idGroup =";
	private static String close_run="UPDATE SERPOSCOPE.run SET dateStop=? WHERE isnull(dateStop)";
	private static String reopen_run="UPDATE SERPOSCOPE.run SET dateStop=null WHERE idRun=?";
	private static String get_checkId_matching_run = "select idCheck from SERPOSCOPE.check where idRun=?";
	private static String missing_keywords = "select * from SERPOSCOPE.keyword where idKeyword not in (select idKeyword from SERPOSCOPE.rank where idCheck=?)";

	public static void instantiante_connection() throws SQLException{
		connect = DriverManager
				.getConnection("jdbc:mysql://localhost/SERPOSCOPE?"
						+ "user=root&password=mogette");
	}

	public static void reopen(String idRun) throws SQLException{
		PreparedStatement statement = connect.prepareStatement(reopen_run);
		statement.setString(1,idRun);
		// ...
		int affectedRows = statement.executeUpdate();
		if (affectedRows == 0) {
			throw new SQLException("Creating user failed, no rows affected.");
		}
	}

	public static String get_check_id(String idRun) throws SQLException{
		PreparedStatement statement = connect.prepareStatement(get_checkId_matching_run);
		statement.setString(1,idRun);
		// resultSet gets the result of the SQL query
		ResultSet resultSet = statement
				.executeQuery();
		String idCheck="";
		while (resultSet.next()) {
			idCheck = resultSet.getString("idCheck");
		}
		return idCheck;
	}

	public static int check_alive_run() throws SQLException{
		Statement statement = connect.createStatement();
		// resultSet gets the result of the SQL query
		ResultSet resultSet = statement
				.executeQuery(selectCurrentRun);
		int counter = 0;
		while (resultSet.next()) {
			counter ++;
		}
		return counter;
	}

	public static  ResultSet search_missing_keywords(String idCheck) throws SQLException{
		PreparedStatement statement = connect.prepareStatement(missing_keywords);
		statement.setString(1,idCheck);
		// resultSet gets the result of the SQL query
		ResultSet resultSet = statement
				.executeQuery();
		return resultSet;
	}

	public static  ResultSet search_keywords(String idGroup) throws SQLException{
		keyword_query=keyword_query+idGroup;
		Statement keyword_statement = connect.createStatement();
		return keyword_statement
				.executeQuery(keyword_query);
	}

	public static ResultSet search_target(String idGroup) throws SQLException{
		Statement target_statement = connect.createStatement();
		// resultSet gets the result of the SQL query
		target_query=target_query+idGroup;
		return  target_statement
				.executeQuery(target_query);
	}
	
	public static ResultSet search_group(String[] groups) throws SQLException{
		// getting our groups
		Statement group_statement = connect.createStatement();
		if (groups.length > 0){
			group_request=group_request+" where name in ('"+Joiner.on("','").join(groups)+"')";
		}
		group_request=group_request+" ORDER BY name desc";
		return group_statement
				.executeQuery(group_request);
	}


	public static void insertKeyword(String idCheck, String idTarget, String idKeyword, int position, String url) throws SQLException {	
		try (
				PreparedStatement statement = connect.prepareStatement(keyword_insert);
				) {
			//(idCheck,idTarget,idKeyword,position,url)
			statement.setString(1,idCheck);
			statement.setString(2, idTarget);
			statement.setString(3, idKeyword);
			statement.setInt(4, position);
			statement.setString(5, url);
			statement.executeUpdate();
		}
	}


	public static void close_current_run() throws SQLException{
		try (
				PreparedStatement statement = connect.prepareStatement(close_run);
				) {
			statement.setDate(1,new java.sql.Date(System.currentTimeMillis()));
			// ...
			statement.executeUpdate();

		}
	}

	public static long insertCheckById(java.sql.Date date, String idGroup, String idRun) throws SQLException {	
		try (
				PreparedStatement statement = connect.prepareStatement(check_insert,
						Statement.RETURN_GENERATED_KEYS);
				) {
			statement.setString(1,idGroup);
			statement.setString(2, idRun);
			statement.setDate(3, date);
			// ...
			statement.executeUpdate();

			try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
				if (generatedKeys.next()) {
					return generatedKeys.getLong(1);         
				}
				else {
					throw new SQLException("Creating user failed, no ID obtained.");
				}
			}
		}
	}

	public static long insertRunById(java.sql.Date date) throws SQLException {	
		try (
				PreparedStatement statement = connect.prepareStatement(runInsertString,
						Statement.RETURN_GENERATED_KEYS);
				) {
			int my_pid = randInt(1,100);
			statement.setDate(1,date);
			statement.setDate(2, null);
			statement.setString(3, "serposcope_"+my_pid+".log");
			statement.setInt(4, my_pid);
			statement.setInt(5, 0);
			// ...
			int affectedRows = statement.executeUpdate();
			if (affectedRows == 0) {
				throw new SQLException("Creating user failed, no rows affected.");
			}
			try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
				if (generatedKeys.next()) {
					return generatedKeys.getLong(1);         
				}
				else {
					throw new SQLException("Creating user failed, no ID obtained.");
				}
			}
		}
	}

	public static void close(){
		if (connect != null) {
			try {
				connect.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static int randInt(int min, int max) {
		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}


}
