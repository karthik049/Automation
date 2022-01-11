package testcases.ServiceTitan;
import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class SnowFlake_RowCount_Master  extends BaseClass{


	int SF_RAW_RowCount=0;
	int SF_DataMart_RowCount=0;
	int SF_Curated_RowCount=0;
	int SF_APIMAT_RowCount=0;
	//For API Business logic // Curated Zone Not updated by API since Record count same in API and BKP
	int SF_Curated_RowCount_BKP=0;

	@Test(enabled = true, dataProvider = "RowCount_BKP_API")
	public void Testcase_RowCount_validation(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String RECORD_UPDATED_BY, String flag) throws SQLException
	{
		if(flag.equals("Y"))
		{
			switch(Type.trim())
			{
			case "BKPOnly_Table":
				validate_RowCount_BKPOnly(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable, RECORD_UPDATED_BY,flag);
				break;
			case "BKP_Table":
				validate_RowCount_BKP(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable, RECORD_UPDATED_BY,flag);
				break;
			case "API_Table":
				validate_RowCount_API(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable, RECORD_UPDATED_BY,flag);
				break; 
			default : 
				PrintUtils.logInfo("Please give correct Type in Excel sheet");
				break;
			}
		}
	}

	public void validate_RowCount_BKPOnly(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String RECORD_UPDATED_BY,String flag) throws SQLException
	{
		ExtentReport.setTestName(TenantName+"_"+RawTable +" ("+Type+")", "Row Count validation for RAW VS Datamart VS Curated for all "+ Type +" Tables");

		// RAW Count
		SF_RAW_RowCount= snowflake.readDB("SELECT count(*) as TOTAL FROM "+SRCSchema+"."+RawTenant+"."+RawTable);

		//Datamart Active_Flag=Y
		SF_DataMart_RowCount= snowflake.readDB("select count(*) as TOTAL from "+StageSchema+"."+DataMartTenant+"."+DatamartTable+" Where ACTIVE_FLAG='Y'");

		//Curated Tenant Type filter 
		SF_Curated_RowCount=  snowflake.readDB("select count(*) as TOTAL from "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" where tenant_type='"+TenantName+"'"); // and RECORD_UPDATED_BY='"+RECORD_UPDATED_BY+"'

		//if(SF_Curated_RowCount >= SF_DataMart_RowCount && SF_DataMart_RowCount >= SF_RAW_RowCount)
		if(SF_RAW_RowCount >= SF_DataMart_RowCount && SF_DataMart_RowCount == SF_Curated_RowCount)
		{
			PrintUtils.logMsg("Row Count of Table "+RawTable +" is matched for RAW :: "+ SF_RAW_RowCount +" DataMart :: "+ SF_DataMart_RowCount + " CURATED :: " + SF_Curated_RowCount);
		}else 
		{
			PrintUtils.logError("Row Count of Table "+RawTable+" is not matched for RAW :: "+ SF_RAW_RowCount +" DataMart :: "+ SF_DataMart_RowCount + " CURATED :: " + SF_Curated_RowCount);
		}

		sa.assertAll();
		ExtentReport.endReport();
	}

	public void validate_RowCount_BKP(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String RECORD_UPDATED_BY,String flag) throws SQLException
	{
		String tableName=RawTable;
		ExtentReport.setTestName(TenantName+"_"+RawTable +" ("+Type+")", "Row Count validation for RAW VS Datamart VS Curated for all "+ Type +" Tables");

		// RAW Count
		SF_RAW_RowCount= snowflake.readDB("SELECT count(*) as TOTAL FROM "+SRCSchema+"."+RawTenant+"."+RawTable);

		//Datamart Active_Flag=Y
		SF_DataMart_RowCount= snowflake.readDB("select count(*) as TOTAL from "+StageSchema+"."+DataMartTenant+"."+DatamartTable+" Where ACTIVE_FLAG='Y'");

		//Curated Tenant Type filter 
		SF_Curated_RowCount=  snowflake.readDB("select count(*) as TOTAL from "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" where tenant_type='"+TenantName+"' and RECORD_UPDATED_BY='"+RECORD_UPDATED_BY+"'");


		// Validation 1 (APIMAT vs Datamart)
		if(SF_RAW_RowCount >= SF_DataMart_RowCount)
		{
			PrintUtils.logMsg("Row Count of Table "+tableName +" is matched for RAW :: "+ SF_RAW_RowCount +" DataMart :: "+ SF_DataMart_RowCount);
		}else 
		{
			PrintUtils.logError("Row Count of Table "+tableName+" is not matched for RAW :: "+ SF_RAW_RowCount +" DataMart :: "+ SF_DataMart_RowCount);
		}

		// Validation 2 (Datamart vs Curated)
		if(SF_DataMart_RowCount == SF_Curated_RowCount)
		{
			PrintUtils.logMsg("Row Count of Table "+tableName +" is matched for DataMart :: "+ SF_DataMart_RowCount + " CURATED :: " + SF_Curated_RowCount);
		}else 
		{
			// Business Rule - Sub query

			String SubQuery= "SELECT ID FROM "+SRCSchema+"."+RawTenant+"."+RawTable;
			SF_Curated_RowCount_BKP=  snowflake.readDB("select count(*) as TOTAL from "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" WHERE TENANT_TYPE='"+TenantName+"' AND ID in ("+SubQuery+")");
			if(SF_RAW_RowCount==SF_Curated_RowCount_BKP)
			{
				PrintUtils.logMsg("Row Count of Table "+tableName +" is matched for RAW (Sub Query) :: "+ SF_RAW_RowCount + " CURATED (Sub Query):: " + SF_Curated_RowCount_BKP);	
			}
			else
			{
				PrintUtils.logError("Row Count of Table "+tableName+" is not matched for RAW (Sub Query):: "+ SF_RAW_RowCount + " CURATED (Sub Query) :: " + SF_Curated_RowCount_BKP);
			}
		}


		sa.assertAll();
		ExtentReport.endReport();
	}

	public void validate_RowCount_API(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String RECORD_UPDATED_BY,String flag) throws SQLException
	{
		String tableName=RawTable.split("_")[2];
		ExtentReport.setTestName(TenantName+"_"+tableName +" ("+Type+")", "Row Count validation for APIMAT VS Datamart VS Curated for all "+ Type +" Tables");

		// API MAT Count
		String APIMATQuery="SELECT count(*) as TOTAL FROM "+SRCSchema+"."+RawTenant+"."+RawTable +" WHERE TENANT_NAME='"+TenantName+"'";
		SF_APIMAT_RowCount= snowflake.readDB(APIMATQuery);

		//Datamart Active_Flag=Y
		String DataMartQuery="select count(*) as TOTAL from "+StageSchema+"."+DataMartTenant+"."+DatamartTable+" WHERE ACTIVE_FLAG='Y'";
		SF_DataMart_RowCount= snowflake.readDB(DataMartQuery);

		//Curated Tenant Type filter
		String CuratedQuery="select count(*) as TOTAL from "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" WHERE TENANT_TYPE='"+TenantName+"' and RECORD_UPDATED_BY='"+RECORD_UPDATED_BY+"'";
		SF_Curated_RowCount=  snowflake.readDB(CuratedQuery);


		// APIMAT :: 31 
		// DataMart :: 31 
		// CURATED :: 0

		// Validation 1 (APIMAT vs Datamart)
		if(SF_APIMAT_RowCount >= SF_DataMart_RowCount)
		{
			PrintUtils.logMsg("Row Count of Table "+tableName +" is matched for APIMAT :: "+ SF_APIMAT_RowCount +" DataMart :: "+ SF_DataMart_RowCount);
		}else 
		{
			PrintUtils.logError("Row Count of Table "+tableName+" is not matched for APIMAT :: "+ SF_APIMAT_RowCount +" DataMart :: "+ SF_DataMart_RowCount);
		}

		// Validation 2 (Datamart vs Curated)
		if(SF_DataMart_RowCount == SF_Curated_RowCount)
		{
			PrintUtils.logMsg("Row Count of Table "+tableName +" is matched for DataMart :: "+ SF_DataMart_RowCount + " CURATED :: " + SF_Curated_RowCount);
		}else 
		{
			// Business Rule - BKP VS API Count match then API wont update the Record_Updated_BY field
			//Check Curated Table is not updated by API since it have same Count in API vs BKP

			// API -

			// Bala --> Bala1 | API ETL | BKP | Created on | Modi on (MD) API 
			//String SubQuery="select ID from CURATED_DATAMART."+DataMartTenant+"."+DatamartTable+" WHERE ACTIVE_FLAG='Y'";
			String SubQuery= "SELECT ID FROM "+SRCSchema+"."+RawTenant+"."+RawTable +" WHERE TENANT_NAME='"+TenantName+"'";
			SF_Curated_RowCount_BKP=  snowflake.readDB("select count(*) as TOTAL from "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" WHERE TENANT_TYPE='"+TenantName+"' AND ID in ("+SubQuery+")");
			if(SF_APIMAT_RowCount==SF_Curated_RowCount_BKP)
			{
				PrintUtils.logMsg("Row Count of Table "+tableName +" is matched for Sub Query RAW :: "+ SF_APIMAT_RowCount + " CURATED (Sub Query):: " + SF_Curated_RowCount_BKP);	
			}
			else
			{
				PrintUtils.logError("Row Count of Table "+tableName+" is not matched for Sub Query RAW :: "+ SF_APIMAT_RowCount + " CURATED (Sub Query) :: " + SF_Curated_RowCount_BKP);
			}
		}
		sa.assertAll();
		ExtentReport.endReport();
	}


	@DataProvider(name="RowCount_BKP_API",parallel = false)
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ServiceTitan_RowCountValidation_Snowflake", "RowCount_MasterSheet");
	}
}
