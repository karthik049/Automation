package testcases.SQLToRAW;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class RowCount extends BaseClass{


	@Test(enabled = false)
	public void validateRowCount()
	{
		ExtentReport.setTestName("Row Count of AccountingTransaction", "Validate the row count of each table");

		int SQL_rowCount= sql.readDB("SELECT COUNT(*) FROM [St_Backup_Horizon].[horizonservices].[AccountingTransaction]");
		int SnowFlake_rowCount=  snowflake.readDB("SELECT COUNT(*) as total FROM \"DEV_ENT_RAW\".\"ST_HORIZONSERVICES_MATILLION\".\"ACCOUNTINGTRANSACTION\"");

		if(SQL_rowCount == SnowFlake_rowCount)
		{
			PrintUtils.logMsg("Row count in Database matched :: " +"SQL_rowcount is:: "+ SQL_rowCount + " SnowFlakeCount is :: "+ SnowFlake_rowCount);
		}else 
		{
			PrintUtils.logError("Row count in Database not matched :: " +"SQL_rowcount is:: "+ SQL_rowCount + " SnowFlakeCount is :: "+ SnowFlake_rowCount);
		}
		ExtentReport.endReport();
	}


	@Test(enabled = true, dataProvider = "RowSheet")
	public void validateRowCountOfEachTable(String TestName, String Tenant, String Validation, String srcQuery, String destQuery) throws SQLException
	{
		ExtentReport.setTestName(TestName+"_"+Tenant, Validation+" :: "+ Tenant);

		ResultSet SQL_RS= sql.readDBAndReturnResultSet(srcQuery);

		while (SQL_RS.next()) 
		{
			sqlMap.put(SQL_RS.getString(1), ""+SQL_RS.getInt(2));
		}
		PrintUtils.logMsg("sqlMap:: "+sqlMap.size());
		PrintUtils.logMsg("sqlMap :: "+sqlMap.entrySet());

		ResultSet Snow_RS_rowCount=  snowflake.readDBAndReturnResultSet(destQuery);


		while (Snow_RS_rowCount.next()) 
		{
			snowMap.put(Snow_RS_rowCount.getString(1),Snow_RS_rowCount.getString(2));
		}

		PrintUtils.logMsg("snowMap:: "+snowMap.size());
		PrintUtils.logMsg("snowMap:: "+snowMap.entrySet());

		sqlMap.forEach((SQLkey,SQLvalue)->
		{ 
			String snowValue =  snowMap.get(SQLkey.toUpperCase()); 
			if(SQLvalue.equals(snowValue))
			{
				PrintUtils.logMsg("Row count matched for table "+SQLkey+" | "+SQLvalue+" | "+snowValue);
			}
			else
			{
				PrintUtils.logError("Row count mismatch for table "+SQLkey+" | "+SQLvalue+" | "+snowValue); 
			}
		});


		//		for (String sqlkey : sqlMap.keySet()) 
		//		{
		//			actualList.add(sqlkey.toLowerCase());
		//		}
		//		for (String snowkey : snowMap.keySet()) 
		//		{
		//			expectedList.add(snowkey.toLowerCase());
		//		}
		//		
		//		for (String string : expectedList) 
		//		{
		//			if(!actualList.contains(string))
		//			{
		//				System.out.println("NO value -- "+ string);
		//				System.out.println("matillion_master_load_test"+snowMap.get(string.toUpperCase()));
		//			}
		//		}
		//		
		sa.assertAll();
		ExtentReport.endReport();
	}


	@DataProvider(name="RowSheet")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("SQL_SnowFlake_Query_JDBC", "Row_TC");
	}


}
