package testcases.ServiceTitan;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import common.Validator;
import testcases.BaseClass;

public class SnowFlake_DataCheck_Master  extends BaseClass{


	private static ArrayList<String> RAWDataMartList=new ArrayList<String>();
	private static ArrayList<String> DataMartCuratedList=new ArrayList<String>();
	private static ArrayList<String> RAWColumnList=new ArrayList<String>();


	@Test(enabled = true, dataProvider = "DataCheck_BKP_API")
	public void Testcase_RowCount_validation(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String RECORD_UPDATED_BY, String flag) throws SQLException
	{
		if(flag	.equals("Y"))
		{
			switch(Type.trim())
			{
			case "BKPOnly_Table":
				validate_DataComparison(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable, RECORD_UPDATED_BY,flag);
				break;
			case "BKP_Table":
				validate_DataComparison(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable, RECORD_UPDATED_BY,flag);
				break;
			case "API_Table":
				validate_DataComparison(testID, Type, TenantName, RawTenant, DataMartTenant, Curated_Tenant, RawTable, DatamartTable, CuratedTable, RECORD_UPDATED_BY,flag);
				break;
			default : 
				PrintUtils.logInfo("Please give correct Type in Excel sheet");
				break;
			}
		}
	}

	public void validate_DataComparison(String testID, String Type, String TenantName, String RawTenant, String DataMartTenant, String Curated_Tenant, String RawTable, String DatamartTable,String CuratedTable, String RECORD_UPDATED_BY,String flag) throws SQLException
	{
		actualList.clear();
		RAWDataMartList.clear();
		DataMartCuratedList.clear();
		String RawQuery=null;
		String DataMart=null;
		String Curated=null;

		ExtentReport.setTestName(TenantName+"_"+RawTable +" ("+Type+")", "Data validation for RAW VS Datamart VS Curated for all "+ Type +" Tables");

		// Get column name and check all records,
		ResultSet RS_RAW = snowflake.readDBAndReturnResultSet("SELECT COLUMN_NAME FROM "+SRCSchema+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+RawTable+"'and TABLE_SCHEMA='"+RawTenant+"'");
		StringBuilder r= new StringBuilder();

		actualList.add("VALUE");
		actualList.add("STATUS");
		actualList.add("UNION");
		actualList.add("CODE");
		actualList.add("END");
		actualList.add("START");
		actualList.add("TO");
		actualList.add("FROM");
		actualList.add("ORDER");

		while(RS_RAW.next())
		{
			String value =RS_RAW.getString(1).toUpperCase();

			String replace = new String(value);
			if (!replace.contains(".") && !actualList.contains(value))
			{
				r.append(value+",");
			}
		}

		String src_Columns = Validator.removeLastChars(r.toString(), 1);
		//RawQuery = "SELECT Distinct ID FROM "+DevSchema.DEV_ENT_RAW+"."+RawTenant+"."+RawTable;
		RawQuery = "SELECT Distinct "+src_Columns+" FROM "+SRCSchema+"."+RawTenant+"."+RawTable;
		DataMart ="select Distinct "+src_Columns+" from "+StageSchema+"."+DataMartTenant+"."+DatamartTable+" Where ACTIVE_FLAG='Y'";
		Curated ="select Distinct "+src_Columns+" from "+TargetSchema+"."+Curated_Tenant+"."+CuratedTable+" where tenant_type='"+TenantName+"' and RECORD_UPDATED_BY='"+RECORD_UPDATED_BY+"'";

		//Check 1 RAW - Datamart
		String RAW_DataMart=RawQuery + " MINUS " + DataMart;

		int RAWDATAMART_COUNT=snowflake.readDB("select count(1) as Total from ( "+RAW_DataMart+") ");
				
		//int RD_Size=RAW_DataMart_RS.getFetchSize();
		int RD_Size=RAWDATAMART_COUNT;
		if(RD_Size!=0)
		{
			ResultSet RAW_DataMart_RS = snowflake.readDBAndReturnResultSet(RAW_DataMart);
			PrintUtils.logError("Mismatched row count :: " + RD_Size);
			while(RAW_DataMart_RS.next())
			{
				String value =RAW_DataMart_RS.getString(1).toUpperCase();
				RAWDataMartList.add(value);
			}
			PrintUtils.logInfo("Record not found in DataMart (RAW vs Datamart Comparison failed) --> "+TenantName+"_"+RawTable+" :: "+ RAWDataMartList);
		}

		//Check 2 DataMart - Curated 
		String Datamart_Curated= DataMart + " MINUS " + Curated;
		
		int DATAMARTCURATED_COUNT=snowflake.readDB("select count(1) as Total from ( "+Datamart_Curated+") ");
		
		
		//int DC_Size=Datamart_Curated_RS.getFetchSize();
		int DC_Size=DATAMARTCURATED_COUNT;
		if(DC_Size!=0)
		{
			ResultSet Datamart_Curated_RS = snowflake.readDBAndReturnResultSet(Datamart_Curated);
			PrintUtils.logError("Mismatched row count :: " + DC_Size);
			while(Datamart_Curated_RS.next())
			{
				String value =Datamart_Curated_RS.getString(1).toUpperCase();
				DataMartCuratedList.add(value);
			}
			PrintUtils.logInfo("Record not found in Curated (DataMart vs Curated Comparison failed) --> "+TenantName+"_"+RawTable+" :: "+ DataMartCuratedList);
		}

		sa.assertAll();
		ExtentReport.endReport();
	}



	@DataProvider(name="DataCheck_BKP_API")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ServiceTitan_DataValidation_Snowflake", "DataCheck_MasterSheet");
	}
}
