package testcases.Dev;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.AfterTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class DIMTableCounts extends BaseClass {
	
	List<String> failures = new ArrayList<>();
	
	@Test(enabled = false, dataProvider = "DIMRowCount")
	public void getDIMTableCounts(String TenantName, String TableName, String flag) throws Exception
	{
		if(flag.equals("Y"))
		{
		ExtentReport.setTestName(TenantName+"_"+TableName, "Row Count for DIM tables");

		// RAW Count
		int SF_DIM_Count=  snowflake.readDB("select count(*) as TOTAL from DEV_ENT_CURATED.MASTER_DIMENSIONS."+TableName+" where tenant_type='"+TenantName+"'");
	
		if(SF_DIM_Count > 0)
		{
			//PrintUtils.logMsg(TenantName +" || "+ TableName +" Row Count for Table is >=  " +SF_DIM_Count);
		}else if(SF_DIM_Count == 0 || SF_DIM_Count<0)
		{
			failures.add(TenantName +"_"+ TableName);
			PrintUtils.logError(TenantName +" || "+ TableName +" Row Count for Table is <=  " +SF_DIM_Count);
		}
		sa.assertAll();
		ExtentReport.endReport();
		}
	}
	
	@AfterTest
	public void AfterTest() 
	{
		ExtentReport.setTestName("Failures", "Row Count for DIM tables");
		PrintUtils.logMsg(""+failures);
		ExtentReport.endReport();
	}
	
	@Test(enabled = true,dataProvider = "DIMDupcheck")
	public void getDIMTableDuplicates(String TableName, String flag) throws Exception
	{
		if(flag.equals("Y"))
		{
		int count =0;
		ExtentReport.setTestName(TableName, "Validated Duplicate check");

		// RAW Count
		ResultSet SF_DIM_dup=  snowflake.readDBAndReturnResultSet("select count(*) as Total from (SELECT DISTINCT(TENANT_TYPE) FROM (SELECT concat(id,'_',TENANT_TYPE),TENANT_TYPE ,COUNT(1) FROM DEV_ENT_CURATED.MASTER_DIMENSIONS."+TableName+" GROUP BY 1,2 HAVING COUNT(1) > 1) A)");
	
		while(SF_DIM_dup.next())
		{
			count =count+ SF_DIM_dup.getInt("TOTAL");
		}
		if(count == 0)
		{
			PrintUtils.logMsg(TableName +" No Duplicate found");
		}else if(count >0 )
		{
			PrintUtils.logError(TableName +" Contains duplicates ");
		}
		sa.assertAll();
		ExtentReport.endReport();
		}
	}
	

//	@DataProvider(name="DIMRowCount")
//	public Object[][] readDataFromExcel_rowCount()
//	{
//		return DataInputProvider.getData("ServiceTitan_RowCount_DIMTables", "DIMTables");
//	}
	
	@DataProvider(name="DIMDupcheck")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ServiceTitan_RowCount_DIMTables", "DIM_DUPS");
	}
}
