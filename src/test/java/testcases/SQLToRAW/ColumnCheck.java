package testcases.SQLToRAW;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class ColumnCheck extends BaseClass{

	
	@Test(enabled = true, dataProvider = "ColumnSheet")
	  public void validateColumnName(String testcaseName, String tableName, String tenant, String ValidationType, String srcQuery, String destQuery) throws SQLException
	  {
		ExtentReport.setTestName(testcaseName+"_"+tenant, ValidationType);
		//SQL
		ResultSet SQL_RS = sql.readDBAndReturnResultSet(srcQuery);	
		while (SQL_RS.next()) 
		{
			actualList.add(SQL_RS.getString(1).toUpperCase());		
		}
		PrintUtils.logMsg("SQL Column count in Table :: "+actualList.size());
		PrintUtils.logMsg("SQL Column Name from Table ::"+ actualList);
		
		//SnowFlake
		ResultSet SF_RS = snowflake.readDBAndReturnResultSet(destQuery);
		while(SF_RS.next())
		{
			expectedList.add(SF_RS.getString(1).toUpperCase());
		}
		PrintUtils.logMsg("SnowFlake Column count in Table :: "+expectedList.size());
		PrintUtils.logMsg("SnowFlake Column Name from Table :: "+ expectedList);
		
		compareList(actualList, expectedList);
		sa.assertAll();
		ExtentReport.endReport();
	  }
	
	@DataProvider(name="ColumnSheet")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("SQL_SnowFlake_Query_JDBC", "Column_TC");
	}
}
