package testcases.SQLToRAW;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class TableCount extends BaseClass
{
	@Test(enabled = false)
	public void validateCount()
	{
		ExtentReport.setTestName("TableCount_St_Backup_Fenwick", "To verify the Table count of St_Backup_Fenwick in SQL and Snowflake");
		int SQL_rowCount= sql.readDB("SELECT count(*) FROM St_Backup_HDAir.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' and Table_schema='hurleydavidair'");
		int SnowFlake_rowCount=  snowflake.readDB("SELECT COUNT(*) as total FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ST_HDAIR_MATILLION' and table_name not in ('MATILLION_MASTER_LOAD_TEST');");

		if(SQL_rowCount== SnowFlake_rowCount)
		{
			PrintUtils.logMsg("Table count in Database matched :: " +"SQL_rowcount is:: "+ SQL_rowCount + " SnowFlakeCount is :: "+ SnowFlake_rowCount);
		}else 
		{
			PrintUtils.logError("Table count in Database not matched :: " +"SQL_rowcount is:: "+ SQL_rowCount + " SnowFlakeCount is :: "+ SnowFlake_rowCount);
		}

		ExtentReport.endReport();
	}


	@Test(enabled = true, dataProvider = "TableCountData")
	public void validateTableCount(String TestName, String Tenant, String Validation, String srcQuery, String destQuery)
	{
		ExtentReport.setTestName(TestName+"_"+Tenant, Validation+" :: "+ Tenant);

		int SQL_rowCount= sql.readDB(srcQuery);
		int SnowFlake_rowCount=  snowflake.readDB(destQuery);
		if(SQL_rowCount == SnowFlake_rowCount)
		{
			PrintUtils.logMsg("Table count in Database matched :: " +"SQL_TableCount is:: "+ SQL_rowCount + " SnowFlake_TableCount is :: "+ SnowFlake_rowCount);
		}else 
		{
			PrintUtils.logError("Table count in Database not matched :: " +"SQL_TableCount is:: "+ SQL_rowCount + " SnowFlake_TableCount is :: "+ SnowFlake_rowCount);
		}

		sa.assertAll();
		ExtentReport.endReport();
	}

	@DataProvider(name="TableCountData")
	public Object[][] readDataFromExcel()
	{
		return DataInputProvider.getData("SQL_SnowFlake_Query_JDBC", "TableCount_TCs");
	}









}
