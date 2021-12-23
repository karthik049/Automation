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

public class ADP_DataCheck  extends BaseClass{


	private static ArrayList<String> RAWDataMartList=new ArrayList<String>();
	private static ArrayList<String> DataMartCuratedList=new ArrayList<String>();

	@Test(enabled = true, dataProvider = "DataCheck_ADP")
	public void Testcase_RowCount_validation(String testID, String TenantName, String RawSchema, String DataMartSchema, String CuratedSchema, String RawTable, String DatamartTable,String CuratedTable,String flag) throws SQLException
	{
		if(flag	.equals("Y"))
		{
			validate_DataComparison(testID, TenantName, RawSchema, DataMartSchema, CuratedSchema, RawTable, DatamartTable, CuratedTable, flag);
		}
	}

	public void validate_DataComparison(String testID, String TenantName, String RawSchema, String DataMartSchema, String CuratedSchema, String RawTable, String DatamartTable,String CuratedTable,String flag) throws SQLException
	{
		RAWDataMartList.clear();
		DataMartCuratedList.clear();
		String RawQuery=null;
		String DataMart=null;
		String Curated=null;

		ExtentReport.setTestName(TenantName+"_"+CuratedTable.split("_")[1], "Data Compare (WorkerID - PK and FK) for RAW VS Datamart VS Curated for Tables ");

		RawQuery = "SELECT Distinct WORKERID FROM "+SRCSchema+"."+RawSchema+"."+RawTable;
		DataMart ="SELECT Distinct WORKERID FROM "+StageSchema+"."+DataMartSchema+"."+DatamartTable+" Where ACTIVE_FLAG='Y'";
		Curated ="SELECT Distinct WORKERID FROM "+TargetSchema+"."+CuratedSchema+"."+CuratedTable;

		//Check 1 RAW - Datamart
		String RAW_DataMart=RawQuery + " MINUS " + DataMart;

		ResultSet RAW_DataMart_RS = snowflake.readDBAndReturnResultSet(RAW_DataMart);
		int RD_Size=RAW_DataMart_RS.getFetchSize();
		if(RD_Size!=0)
		{
			PrintUtils.logError("Mismatched row count :: " + RD_Size);
			while(RAW_DataMart_RS.next())
			{
				String value =RAW_DataMart_RS.getString(1).toUpperCase();
				RAWDataMartList.add(value);
			}
			PrintUtils.logError("Record not found in DataMart (RAW vs Datamart Comparison failed) --> "+TenantName+"_"+RawTable+" :: "+ RAWDataMartList);
		}

		//Check 2 DataMart - Curated 
		String Datamart_Curated= DataMart + " MINUS " + Curated;
		ResultSet Datamart_Curated_RS = snowflake.readDBAndReturnResultSet(Datamart_Curated);
		int DC_Size=Datamart_Curated_RS.getFetchSize();
		if(DC_Size!=0)
		{
			PrintUtils.logError("Mismatched row count :: " + DC_Size);
			while(Datamart_Curated_RS.next())
			{
				String value =Datamart_Curated_RS.getString(1).toUpperCase();
				DataMartCuratedList.add(value);
			}
			PrintUtils.logError("Record not found in Curated (DataMart vs Curated Comparison failed) --> "+TenantName+"_"+RawTable+" :: "+ DataMartCuratedList);
		}

		sa.assertAll();
		ExtentReport.endReport();
	}



	@DataProvider(name="DataCheck_ADP")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ADP_TestData", "DataCheck");
	}
}
