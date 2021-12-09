package dbdriver;

import java.sql.*;
import common.PrintUtils;

public class SnowFlakeConnector {

	public Connection con=null;
	public Statement stmt=null;
	public ResultSet rs=null;

	
	private  String Server="jdbc:snowflake://hxa25538.us-east-1.snowflakecomputing.com";
	
	//jdbc:sqlserver://172.31.14.175;database=St_Backup_Casteel
	//jdbc:snowflake://hxa25538.us-east-1.snowflakecomputing.com/?warehouse=ANALYSIS_WH&db=DEV_ENT_RAW&schema=ST_CASTEEL_MATILLION&user=ENT_ANALYSTS&ROLE=SYSADMIN

	public void connectToDB() 
	{
		try 
		{
			Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");  
			con=DriverManager.getConnection("jdbc:snowflake://hxa25538.us-east-1.snowflakecomputing.com/?warehouse=COMPUTE_WH&db=DEV_ENT_RAW&user=ENT_ANALYSTS&ROLE=SYSADMIN&password=hZn_D@tfn");
			stmt=con.createStatement();
		}
		catch(ClassNotFoundException e)
		{
			PrintUtils.logError("ClassNotFoundException at connectToDB() method :: "+ e.getMessage());
		}  
		catch(SQLException e)
		{
			PrintUtils.logError("SQLException at connectToDB() method :: "+ e.getMessage());
		}
	}

	public int readDB(String query)
	{

		int count =0;
		try {

			PrintUtils.logMsg("Executing Snowflake query :: "+query);
			stmt=con.createStatement();
			rs=stmt.executeQuery(query);
			//ResultSetMetaData resultSetMetaData = rs.getMetaData();

			//System.out.println("Number of columns=" + resultSetMetaData.getColumnCount());
			//System.out.println(resultSetMetaData.getColumnName(1));

			while(rs.next())
			{
				count =count+ rs.getInt("TOTAL");
				PrintUtils.logMsg("Table Count in Snowflake:: "+count);
			}

		} catch (Exception e)
		{
			PrintUtils.logError("Snowflake - SQLException at readDB() method :: "+ e.getMessage());
		}  

		return count;
	}

	public ResultSet readDBAndReturnResultSet(String query) 
	{
		try 
		{
			PrintUtils.logMsg("Executing Snowflake query :: "+query);
			rs=stmt.executeQuery(query); 
		}
		catch (SQLException e)
		{
			PrintUtils.logError("Snowflake - SQLException at readDBAndReturnResultSet() method :: "+ e.getMessage());
		}  
		return rs;
	}

	public  void closeDBConnection() 
	{
		try 
		{
			con.close();  
		}
		catch (SQLException e)
		{
			PrintUtils.logError("SQLException at closeDBConnection() method :: "+ e.getMessage());
		}
	}

}
