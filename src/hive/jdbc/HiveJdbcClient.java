package hive.jdbc;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hadoop.hive.jdbc.HiveStatement;

/**
 *
 * @author ych0112xzz
 */
public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		// replace "hive" here with the name of the user the queries should run
		// as
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://datanode4:10000/default", "yangcheng", "");
		
		HiveStatement stmt = (HiveStatement) con.createStatement();
		String sql1="select * from company ";
		ResultSet res1 = stmt.executeQuery(sql1);
		while (res1.next()) {
			System.out.println(res1.getString(1));
		}
		String tableName = "ychuserflowtest";
		stmt.execute("drop table if exists " + tableName);
		String createtablesql = "create table "
				+ tableName
				+ "\n("
				+ "flow_start_time string,\n"
				+ "flow_end_time string, \n"
				+ "user_account string,\n"
				+ "imei_or_meid string,\n"
				+ "lac_or_sid_nid string,\n"
				+ "ci_or_bsid string,\n"
				+ "user_ip string,\n"
				+ "server_ip string,\n"
				+ "ip_protocal string,\n"
				+ "user_port string,\n"
				+ "server_port string,\n"
				+ "up_byte_num string,\n"
				+ "down_byte_num string,\n"
				+ "real_visited_host string,\n"
				+ " request_uri string,\n"
				+ "response_status string,\n"
				+ "response_duration string,\n"
				+ "response_content_length string,\n"
				+ "main_service_id string,\n"
				+ "minor_service_id string,\n"
				+ "date string\n)"
				+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE\n";
		System.out.println(createtablesql);
		stmt.execute(createtablesql);
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// load data into table // NOTE: filepath has to be local to the hive
		// server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/user/yangcheng/userflow1.csv";
		sql = "load data inpath '" + filepath + "' OVERWRITE INTO table " + tableName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);

		// select * query 
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getString(1)) + "\t"
					+ res.getString(2));
		}

		// regular hive query sql = "select count(1) from " + tableName;
//System.out.println("Running: " + sql);
//		res = stmt.executeQuery(sql);
//		while (res.next()) {
//			System.out.println(res.getString(1));
//		}

	}
}