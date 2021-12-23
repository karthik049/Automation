package testcases.ADP;
import java.sql.SQLException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import common.DataInputProvider;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class ADP_RowCount  extends BaseClass{


	int SF_RAW_RowCount=0;
	int SF_DataMart_RowCount=0;
	int SF_Curated_RowCount=0;


	@Test(enabled = true, dataProvider = "RowCount_ADP")
	public void Testcase_ADP(String testID, String TenantName, String RawSchema, String DataMartSchema, String CuratedSchema, String RawTable, String DatamartTable,String CuratedTable,String flag) throws SQLException
	{
		if(flag.equals("Y"))
		{
			validate_RowCount(testID, TenantName, RawSchema, DataMartSchema, CuratedSchema, RawTable, DatamartTable, CuratedTable, flag);
		}
	}

	public void validate_RowCount(String testID, String TenantName, String RawSchema, String DataMartSchema, String CuratedSchema, String RawTable, String DatamartTable,String CuratedTable,String flag) throws SQLException
	{
		ExtentReport.setTestName(TenantName+"_"+CuratedTable.split("_")[1], "Row Count validation for RAW VS Datamart VS Curated for Tables ");

		// RAW Count
		SF_RAW_RowCount= snowflake.readDB("SELECT count(*) as TOTAL FROM "+SRCSchema+"."+RawSchema+"."+RawTable);

		//Datamart Active_Flag=Y
		SF_DataMart_RowCount= snowflake.readDB("select count(*) as TOTAL from "+StageSchema+"."+DataMartSchema+"."+DatamartTable+" Where ACTIVE_FLAG='Y'");

		//Curated Tenant Type filter 
		SF_Curated_RowCount=  snowflake.readDB("select count(*) as TOTAL from "+TargetSchema+"."+CuratedSchema+"."+CuratedTable);

		if(SF_RAW_RowCount == SF_DataMart_RowCount && SF_DataMart_RowCount == SF_Curated_RowCount)
		{
			PrintUtils.logMsg("Row Count of Table "+CuratedTable.split("_")[1] +" is matched for RAW :: "+ SF_RAW_RowCount +" DataMart :: "+ SF_DataMart_RowCount + " CURATED :: " + SF_Curated_RowCount);
		}else 
		{
			PrintUtils.logError("Row Count of Table "+CuratedTable.split("_")[1]+" is not matched for RAW :: "+ SF_RAW_RowCount +" DataMart :: "+ SF_DataMart_RowCount + " CURATED :: " + SF_Curated_RowCount);
		}

		sa.assertAll();
		ExtentReport.endReport();
	}



	@DataProvider(name="RowCount_ADP")
	public Object[][] readDataFromExcel_rowCount()
	{
		return DataInputProvider.getData("ADP_TestData", "RowCount");
	}
}
