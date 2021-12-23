package testcases.ServiceTitan;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class SnowFlake_Duplicate_Master  extends BaseClass{


	@Test(enabled = true, dataProvider = "Duplicate_BKP_API")
	public void Testcase_DuplicateData(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String flag) throws SQLException
	{
		if(flag.equals("Y"))
		{
			validate_DuplicateCheck(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable,flag);
		}
	}

	public void validate_DuplicateCheck(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable,String flag) throws SQLException
	{
		String RawQuery=null;
		String DataMart=null;
		String Curated=null;

		ExtentReport.setTestName(TenantName+"_"+RawTable +" ("+Type+")", "Duplicate records validation for RAW VS Datamart VS Curated for all "+ Type +" Tables");

		RawQuery = "SELECT ID, COUNT(1) FROM "+SRCSchema+"."+RawTenant+"."+RawTable +" GROUP BY 1 HAVING COUNT(1)>1";
		DataMart ="SELECT ID, COUNT(1) FROM "+StageSchema+"."+DataMartTenant+"."+DatamartTable+" WHERE ACTIVE_FLAG='Y'  GROUP BY 1 HAVING COUNT(1)>1";
		Curated= "SELECT ID, COUNT(1) FROM "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" WHERE TENANT_TYPE='"+TenantName+"'  GROUP BY 1 HAVING COUNT(1)>1";

		ResultSet RAW_RS = snowflake.readDBAndReturnResultSet(RawQuery);
		ResultSet DataMart_RS = snowflake.readDBAndReturnResultSet(DataMart);
		ResultSet Curated_RS = snowflake.readDBAndReturnResultSet(Curated);

		//RAW
		int Raw_Size=RAW_RS.getFetchSize();
		if(Raw_Size!=0)
		{
			actualList.clear();
			PrintUtils.logError("Duplicate records found RAW table "+TenantName + "_"+RawTable+ " and duplicate count is  :: "+ Raw_Size);
			while(RAW_RS.next())
			{
				String value =RAW_RS.getString(1).toUpperCase();
				actualList.add(value);
			}
			PrintUtils.logInfo("Duplciate Record Details"+TenantName+"_"+RawTable+" :: "+ actualList);
		}
		//Datamart
		int DM_Size=DataMart_RS.getFetchSize();
		if(DM_Size!=0)
		{
			actualList.clear();
			PrintUtils.logError("Duplicate records found DataMart table "+TenantName + "_"+DatamartTable+ " and duplicate count is  :: "+ DM_Size);
			while(DataMart_RS.next())
			{
				String value =DataMart_RS.getString(1).toUpperCase();
				actualList.add(value);
			}
			PrintUtils.logInfo("Duplciate Record Details"+TenantName+"_"+DatamartTable+" :: "+ actualList);
		}
		//Curated
		int CR_Size=Curated_RS.getFetchSize();
		if(CR_Size!=0)
		{
			actualList.clear();
			PrintUtils.logError("Duplicate records found DataMart table "+TenantName + "_"+CuratedTable+ " and duplicate count is  :: "+ CR_Size);
			while(Curated_RS.next())
			{
				String value =Curated_RS.getString(1).toUpperCase();
				actualList.add(value);
			}
			PrintUtils.logInfo("Duplciate Record Details"+TenantName+"_"+CuratedTable+" :: "+ actualList);
		}

		sa.assertAll();
		ExtentReport.endReport();
	}


	@DataProvider(name="Duplicate_BKP_API")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ServiceTitan_DuplicateValidation_Snowflake", "Duplicate_MasterSheet");
	}
}
