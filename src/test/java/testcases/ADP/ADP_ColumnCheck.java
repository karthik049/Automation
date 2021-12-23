package testcases.ADP;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class ADP_ColumnCheck  extends BaseClass{


	private static ArrayList<String> RAWColumnList=new ArrayList<String>();
	private static ArrayList<String> DatamartColumnList=new ArrayList<String>();
	private static ArrayList<String> CuratedColumnList=new ArrayList<String>();

	
	@Test(enabled = true, dataProvider = "ColumnName_ADP")
	public void Testcase_ColumnValidation(String testID,  String TenantName, String RawSchema, String DataMartSchema, String CuratedSchema, String RawTable, String DatamartTable,String CuratedTable,String Flag) throws SQLException
	{
		if(Flag.equalsIgnoreCase("Y"))
		{
			ColumnName_validation(testID, TenantName, RawSchema, DataMartSchema, CuratedSchema, RawTable, DatamartTable, CuratedTable, Flag);
		}
	}
	
	public void ColumnName_validation(String testID,  String TenantName, String RawSchema, String DataMartSchema, String CuratedSchema, String RawTable, String DatamartTable,String CuratedTable,String Flag) throws SQLException
	{
		RAWColumnList.clear();
		DatamartColumnList.clear();
		CuratedColumnList.clear();

		ExtentReport.setTestName(TenantName+"_"+CuratedTable.split("_")[1], "Column validation for RAW VS Datamart VS Curated for Tables ");

		// RAW Column
		ResultSet RS_RAW = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+SRCSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+RawTable+"'and TABLE_SCHEMA='"+RawSchema+"'");
		while(RS_RAW.next())
		{
			String value =RS_RAW.getString(1).toUpperCase();
			RAWColumnList.add(value);
		}

		// Datamart Column
		ResultSet RS_Datamart = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+StageSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+DatamartTable+"'and TABLE_SCHEMA='"+DataMartSchema+"'");
		while(RS_Datamart.next())
		{
			String value1 =RS_Datamart.getString(1).toUpperCase();			
			DatamartColumnList.add(value1);
		}

		// Curated Column
		ResultSet RS_Curated = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+TargetSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+CuratedTable+"'and TABLE_SCHEMA='"+CuratedSchema+"'");
		while(RS_Curated.next())
		{
			String value2 =RS_Curated.getString(1).toUpperCase();			
			CuratedColumnList.add(value2);
		}

		PrintUtils.logMsg("Column names for RAW Table --> "+ RawTable +" :: "+ RAWColumnList);
		PrintUtils.logMsg("Column names for DataMart Table --> "+ DatamartTable +" :: "+ DatamartColumnList);
		PrintUtils.logMsg("Column names for Curated Table --> "+ CuratedTable +" :: "+ CuratedColumnList);

		// Checkpoint : 1 

		for (String column_raw: RAWColumnList)
		{
			if(DatamartColumnList.contains(column_raw) && CuratedColumnList.contains(column_raw))
			{
			    PrintUtils.logMsg("Column Name found in Datamart and Curated Table :: "+ column_raw);
			}else 
			{
				PrintUtils.logError("Column Name not found in Datamart and Curated Table :: "+ column_raw);
			}
		}

		// Checkpoint : 2 - Check datamart is having additional columns in table
		if(DatamartColumnList.contains("MASTER_ID") && DatamartColumnList.contains("ACTIVE_FLAG") && 
				DatamartColumnList.contains("RECORD_CREATE_DATE") && DatamartColumnList.contains("RECORD_UPDATE_DATE"))
		{
			PrintUtils.logMsg("Datamart tables contains additional column name like [MASTER_ID, ACTIVE_FLAG, RECORD_CREATE_DATE, RECORD_UPDATE_DATE]");
		}else 
		{
			PrintUtils.logError("Datamart tables not contains additional column name like [MASTER_ID, ACTIVE_FLAG, RECORD_CREATE_DATE, RECORD_UPDATE_DATE]");
		}


		// Checkpoint : 3 - Check Curated ColumnList  is having additional columns in table

		if(CuratedColumnList.contains("RECORD_CREATE_DATE") && CuratedColumnList.contains("RECORD_UPDATE_DATE"))
		{
			PrintUtils.logMsg("Curated tables contains additional column name like [RECORD_CREATE_DATE, RECORD_UPDATE_DATE]");
		}else 
		{
			PrintUtils.logError("Curated tables not contains additional column name like [RECORD_CREATE_DATE, RECORD_UPDATE_DATE]");
		}

		sa.assertAll();
		ExtentReport.endReport();
	}
	
	@DataProvider(name="ColumnName_ADP")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ADP_TestData", "ColumnCheck");
	}
}
