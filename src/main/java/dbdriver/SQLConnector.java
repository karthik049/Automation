package dbdriver;



import java.sql.*;

import common.PrintUtils;

public class SQLConnector {

	public Connection con=null;
	public Statement stmt=null;
	public ResultSet rs=null;
	
	//jdbc:sqlserver://172.31.14.175;database=St_Backup_Casteel

	public void connectToDB() 
	{
		try 
		{
			Class.forName("com.mysql.cj.jdbc.Driver");  
			con=DriverManager.getConnection("jdbc:sqlserver://172.31.14.175","sa","D1g1TTran$");
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
		int count=0;
		try {
			
			PrintUtils.logMsg("Executing SQL query :: "+query);
			rs=stmt.executeQuery(query); 

			while(rs.next())
			{ 
				count = rs.getInt(1);
				PrintUtils.logMsg("Table Count in SQL Server:: "+ count);
			}
		} catch (SQLException e)
		{
			PrintUtils.logError("SQLException at readDB() method :: "+ e.getMessage());
		}
		return count;  
	}

	public ResultSet readDBAndReturnResultSet(String query) 
	{
		try {
			
			PrintUtils.logMsg("Executing SQL query :: "+query);
			rs=stmt.executeQuery(query); 

		} catch (SQLException e)
		{
			PrintUtils.logError("SQLException at readDB() method :: "+ e.getMessage());
		}
		return rs;  
	}

	
	public  void closeDBConnection() 
	{
		try 
		{
			con.close();  
		}catch (SQLException e)
		{
			PrintUtils.logError("SQLException at closeDBConnection() method :: "+ e.getMessage());
		}
	}

}
