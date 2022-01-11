package testcases.ServiceTitan;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import common.Constants.DevSchema;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class SnowFlake_ColumnValidation_Master  extends BaseClass{


	private static ArrayList<String> RAWColumnList=new ArrayList<String>();
	private static ArrayList<String> APIMATColumnList=new ArrayList<String>();
	private static ArrayList<String> DatamartColumnList=new ArrayList<String>();
	private static ArrayList<String> CuratedColumnList=new ArrayList<String>();


	public void API_Tables_ColumnName_validation(String testID, String Type, String TenantType, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable) throws SQLException
	{
		APIMATColumnList.clear();
		DatamartColumnList.clear();
		CuratedColumnList.clear();

		String tableName=RawTable.split("_")[2];
		ExtentReport.setTestName(TenantType+"_"+tableName +" ("+Type+ ")", "Column validation for APIMAT VS Datamart VS Curated for all API tables");
		// RAW Column
		ResultSet RS_APIMAT = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+SRCSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+RawTable+"'and TABLE_SCHEMA='"+RawTenant+"'");
		while(RS_APIMAT.next())
		{
			String value =RS_APIMAT.getString(1).toUpperCase();
			String replace = new String(value);
			if(replace.contains("."))
			{
				replace= replace.replace(".", "_");
				APIMATColumnList.add(replace);
			}else 
			{
				APIMATColumnList.add(value);
			}
		}

		//Remove Extra Column from APIMAT Schema 
		APIMATColumnList.remove("ETL_CREATED_BY");
		APIMATColumnList.remove("ETL_CREATED_DATE");
		APIMATColumnList.remove("ETL_UPDATED_BY");
		APIMATColumnList.remove("ETL_UPDATED_DATE");
		if(APIMATColumnList.contains("TENANT_NAME"))
		{
			APIMATColumnList.remove("TENANT_NAME");
			APIMATColumnList.add("TENANT_TYPE");
		}

		// Datamart Column
		ResultSet RS_Datamart = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+StageSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+DatamartTable+"'and TABLE_SCHEMA='"+DataMartTenant+"'");
		while(RS_Datamart.next())
		{
			String value1 =RS_Datamart.getString(1).toUpperCase();			
			if(value1.equals("VALUE_") || value1.equals("STATUS_") || value1.equals("UNION_") || value1.equals("CODE_")|| value1.equals("END_")|| value1.equals("START_"))
			{
				String replace1 = new String(value1);
				replace1= replace1.replace("_", "");
				DatamartColumnList.add(replace1);
			}else 
			{
				DatamartColumnList.add(value1);
			}	
		}

		// Curated Column
		ResultSet RS_Curated = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+TargetSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+CuratedTable+"'and TABLE_SCHEMA='"+Curated_Tenant+"'");
		while(RS_Curated.next())
		{
			String value2 =RS_Curated.getString(1).toUpperCase();			
			if(value2.equals("VALUE_") || value2.equals("STATUS_") || value2.equals("UNION_")  || value2.equals("CODE_") || value2.equals("END_")|| value2.equals("START_"))
			{
				String replace2 = new String(value2);
				replace2= replace2.replace("_", "");
				CuratedColumnList.add(replace2);
			}else 
			{
				CuratedColumnList.add(value2);
			}
		}

		PrintUtils.logMsg("Column names for RAW Table --> "+ RawTable +" :: "+ APIMATColumnList);
		PrintUtils.logMsg("Column names for DataMart Table --> "+ DatamartTable +" :: "+ DatamartColumnList);
		PrintUtils.logMsg("Column names for Curated Table --> "+ CuratedTable +" :: "+ CuratedColumnList);

		// Checkpoint : 1 

		for (String column_raw: APIMATColumnList)
		{

			if(DatamartColumnList.contains(column_raw) && CuratedColumnList.contains(column_raw))
			{
				//PrintUtils.logMsg("Column Name found in Datamart and Curated Table :: "+ column_raw);
			}else 
			{
				PrintUtils.logError("Column Name not found in Datamart and Curated Table :: "+ column_raw);
			}
		}

		// Checkpoint : 2 - Check datamart is having additional columns in table

		if(DatamartColumnList.contains("ACTIVE_FLAG") && DatamartColumnList.contains("RECORD_CREATE_DATE")
				&& DatamartColumnList.contains("RECORD_UPDATE_DATE") && DatamartColumnList.contains("MASTER_ID")
				&&  DatamartColumnList.contains("JOB_NAME"))
		{
			PrintUtils.logMsg("Datamart tables contains additional column name like [ACTIVE_FLAG, RECORD_CREATE_DATE, RECORD_UPDATE_DATE, MASTER_ID]");
		}else 
		{
			PrintUtils.logError("Datamart tables not contains additional column name like [ACTIVE_FLAG, RECORD_CREATE_DATE,  RECORD_UPDATE_DATE, MASTER_ID]");
		}


		// Checkpoint : 3 - Check Curated ColumnList  is having additional columns in table

		if(CuratedColumnList.contains("RECORD_CREATE_DATE") && CuratedColumnList.contains("RECORD_UPDATE_DATE")
				&& CuratedColumnList.contains("RECORD_UPDATED_BY") &&  CuratedColumnList.contains("JOB_NAME"))
		{
			PrintUtils.logMsg("Curated tables contains additional column name like [RECORD_CREATE_DATE, RECORD_UPDATE_DATE, RECORD_UPDATED_BY]");
		}else 
		{
			PrintUtils.logError("Curated tables not contains additional column name like [RECORD_CREATE_DATE, RECORD_UPDATE_DATE, RECORD_UPDATED_BY]");
		}

		sa.assertAll();
		ExtentReport.endReport();
	}

	public void BKP_Tables_ColumnName_validation(String testID, String Type, String TenantType, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable) throws SQLException
	{
		RAWColumnList.clear();
		DatamartColumnList.clear();
		CuratedColumnList.clear();

		ExtentReport.setTestName(TenantType+"_"+RawTable +" (" +Type+ ")", "Column validation for RAW VS Datamart VS Curated for all BKP tables");
		// RAW Column
		ResultSet RS_RAW = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+SRCSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+RawTable+"'and TABLE_SCHEMA='"+RawTenant+"'");
		while(RS_RAW.next())
		{
			String value =RS_RAW.getString(1).toUpperCase();
			String replace = new String(value);
			if(replace.contains("."))
			{
				replace= replace.replace(".", "_");
				RAWColumnList.add(replace);
			}else 
			{
				RAWColumnList.add(value);
			}
		}

		// Datamart Column
		ResultSet RS_Datamart = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+StageSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+DatamartTable+"'and TABLE_SCHEMA='"+DataMartTenant+"'");
		while(RS_Datamart.next())
		{
			String value1 =RS_Datamart.getString(1).toUpperCase();			
			if(value1.equals("VALUE_") || value1.equals("STATUS_") || value1.equals("UNION_") || value1.equals("CODE_")|| value1.equals("END_")|| value1.equals("START_"))
			{
				String replace1 = new String(value1);
				replace1= replace1.replace("_", "");
				DatamartColumnList.add(replace1);
			}else 
			{
				DatamartColumnList.add(value1);
			}	
		}

		// Curated Column
		ResultSet RS_Curated = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+TargetSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+CuratedTable+"'and TABLE_SCHEMA='"+Curated_Tenant+"'");
		while(RS_Curated.next())
		{
			String value2 =RS_Curated.getString(1).toUpperCase();			
			if(value2.equals("VALUE_") || value2.equals("STATUS_") || value2.equals("UNION_")  || value2.equals("CODE_") || value2.equals("END_")|| value2.equals("START_"))
			{
				String replace2 = new String(value2);
				replace2= replace2.replace("_", "");
				CuratedColumnList.add(replace2);
			}else 
			{
				CuratedColumnList.add(value2);
			}
		}

		PrintUtils.logMsg("Column names for RAW Table --> "+ RawTable +" :: "+ RAWColumnList);
		PrintUtils.logMsg("Column names for DataMart Table --> "+ DatamartTable +" :: "+ DatamartColumnList);
		PrintUtils.logMsg("Column names for Curated Table --> "+ CuratedTable +" :: "+ CuratedColumnList);

		// Checkpoint : 1 

		for (String column_raw: RAWColumnList)
		{

			if(DatamartColumnList.contains(column_raw) && CuratedColumnList.contains(column_raw))
			{
				//PrintUtils.logMsg("Column Name found in Datamart and Curated Table :: "+ column_raw);
			}else 
			{
				PrintUtils.logError("Column Name not found in Datamart and Curated Table :: "+ column_raw);
			}
		}

		// Checkpoint : 2 - Check datamart is having additional columns in table

		if(DatamartColumnList.contains("ACTIVE_FLAG") && DatamartColumnList.contains("RECORD_CREATE_DATE")
				&& DatamartColumnList.contains("RECORD_UPDATE_DATE") && DatamartColumnList.contains("MASTER_ID")
				&& DatamartColumnList.contains("TENANT_TYPE"))
		{
			PrintUtils.logMsg("Datamart tables contains additional column name like [ACTIVE_FLAG, RECORD_CREATE_DATE, RECORD_UPDATE_DATE, MASTER_ID, TENANT_TYPE]");
		}else 
		{
			PrintUtils.logError("Datamart tables not contains additional column name like [ACTIVE_FLAG, RECORD_CREATE_DATE, RECORD_UPDATE_DATE, RECORD_UPDATE_DATE, MASTER_ID, TENANT_TYPE]");
		}


		// Checkpoint : 3 - Check Curated ColumnList  is having additional columns in table

		if(CuratedColumnList.contains("RECORD_CREATE_DATE") && CuratedColumnList.contains("RECORD_UPDATE_DATE")
				&& CuratedColumnList.contains("RECORD_UPDATED_BY") && CuratedColumnList.contains("TENANT_TYPE"))
		{
			PrintUtils.logMsg("Curated tables contains additional column name like [RECORD_CREATE_DATE, RECORD_UPDATE_DATE, RECORD_UPDATED_BY, TENANT_TYPE]");
		}else 
		{
			PrintUtils.logError("Curated tables not contains additional column name like [RECORD_CREATE_DATE, RECORD_UPDATE_DATE, RECORD_UPDATED_BY, TENANT_TYPE]");
		}

		sa.assertAll();
		ExtentReport.endReport();
	}

	@Test(enabled = true, dataProvider = "ColumnName_API_BKP")
	public void Testcase_ColumnValidation(String testID, String Type, String TenantType, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String flag) throws SQLException
	{
		if(flag.equalsIgnoreCase("Y"))
		{
			switch(Type.trim())
			{
			case "BKPOnly_Table":
				BKP_Tables_ColumnName_validation(testID, Type, TenantType, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable);
				break;
			case "BKP_Table":
				BKP_Tables_ColumnName_validation(testID, Type, TenantType, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable);
				break;
			case "API_Table":
				API_Tables_ColumnName_validation(testID, Type, TenantType, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable);
				break;
			default : 
				PrintUtils.logInfo("Please give correct Type in Excel sheet ::" + Type);
				break;
			}
		}
	}


	@DataProvider(name="ColumnName_API_BKP")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ServiceTitan_ColumnValidation_Snowflake", "ColumnValidation_MasterSheet");
	}
}
