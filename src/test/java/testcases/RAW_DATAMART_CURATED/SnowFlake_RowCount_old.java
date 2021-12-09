package testcases.RAW_DATAMART_CURATED;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import common.ExcelLoader;
import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;

public class SnowFlake_RowCount_old extends BaseClass{

	private final String XLPath="./src/test/resources/TestData/ServiceTitan_TestData.xlsx";

	
//---------------------------------------------------------------------------------------------------------------------------------
	//BKP only
	
	public void validate_RowCount_RAW_DATAMART_CUREATED_BKP() throws Exception
	{
		excel= new ExcelLoader(XLPath, "RowValidation_BKP");
		XSSFSheet sh1 = excel.LoadDataFromSheet();
		for(int i=1; i<=sh1.getLastRowNum();i++)
		{
			String RAW_Table = sh1.getRow(i).getCell(1).getStringCellValue();
			String Datamart_Table = sh1.getRow(i).getCell(3).getStringCellValue();
			String Curated_Table = sh1.getRow(i).getCell(5).getStringCellValue();
			
			String RAW_TenantType = sh1.getRow(i).getCell(2).getStringCellValue();
			String DataMart_TenantType = sh1.getRow(i).getCell(4).getStringCellValue();
			String Curated_TenantType = sh1.getRow(i).getCell(6).getStringCellValue();
		
			ExtentReport.setTestName(Curated_TenantType +" - "+ RAW_Table, "Validate the row count of table from RAW VS DATAMART VS CURATED");
						
			int SF_RAW=       snowflake.readDB("select count(*) as TOTAL from \"DEV_ENT_RAW\"."+"\""+RAW_TenantType+"\""+"."+"\""+RAW_Table+"\""+"");
			int SF_DataMart=  snowflake.readDB(" select count(*) as TOTAL from \"CURATED_DATAMART\"."+"\""+DataMart_TenantType+"\""+ "."+"\""+Datamart_Table+"\"" + "where ACTIVE_FLAG='Y'");
			int SF_CURATED=  snowflake.readDB(" select count(*) as TOTAL from \"DEV_ENT_CURATED\".\"MASTER_DIMENSIONS\"."+"\""+Curated_Table+"\""+ "where tenant_type='"+Curated_TenantType+"'");
			

			//if(SF_RAW == SF_DataMart && SF_DataMart == SF_CURATED)
				
			if(SF_CURATED >= SF_DataMart && SF_DataMart >= SF_RAW)
			{
				PrintUtils.logMsg("Row Count of Table "+RAW_Table +" is matched for RAW :: "+ SF_RAW +" DataMart :: "+ SF_DataMart + " CURATED :: " + SF_CURATED);
			}else 
			{
				PrintUtils.logError("Row Count of Table "+RAW_Table+" is not matched for RAW :: "+ SF_RAW +" DataMart :: "+ SF_DataMart + " CURATED :: " + SF_CURATED);
			}
			
			ExtentReport.endReport();
		}
		
		sa.assertAll();
		
	}
 //---------------------------------------------------------------------------------------------------------------------------------

	
	//API only
	public void validate_RowCount_RAW_DATAMART_CUREATED_API() throws Exception
	{
		excel= new ExcelLoader(XLPath, "RowValidation_API");
		XSSFSheet sh1 = excel.LoadDataFromSheet();
		for(int i=1; i<=sh1.getLastRowNum();i++)
		{
			String DataMart_TenantType = sh1.getRow(i).getCell(1).getStringCellValue();
			String Curated_TenantType = sh1.getRow(i).getCell(3).getStringCellValue();
			String Datamart_Table = sh1.getRow(i).getCell(2).getStringCellValue();
			String Curated_Table = sh1.getRow(i).getCell(4).getStringCellValue();
			
			ExtentReport.setTestName(Curated_TenantType +" - "+ Curated_Table, "Validate the row count of API table from  DATAMART VS CURATED");
						
			int SF_DataMart=  snowflake.readDB(" select count(*) as TOTAL from \"CURATED_DATAMART\"."+"\""+DataMart_TenantType+"\""+ "."+"\""+Datamart_Table+"\"" + "where ACTIVE_FLAG='Y'");
			int SF_CURATED=  snowflake.readDB(" select count(*) as TOTAL from \"DEV_ENT_CURATED\".\"MASTER_DIMENSIONS_BUT\"."+"\""+Curated_Table+"\""+ "where tenant_type='"+Curated_TenantType+"'"+" and Record_Updated_by='API ETL'");
			

			if(SF_DataMart == SF_CURATED)
			{
				PrintUtils.logMsg("Row Count of API Table "+Curated_Table +" is matched for DataMart :: "+ SF_DataMart + " CURATED :: " + SF_CURATED);
			}else 
			{
				PrintUtils.logError("Row Count of API Table "+Curated_Table+" is not matched for DataMart :: "+ SF_DataMart + " CURATED :: " + SF_CURATED);
			}
			
			ExtentReport.endReport();
		}
		
		sa.assertAll();
		
	}
 //---------------------------------------------------------------------------------------------------------------------------------

	//Both BKP and API
		public void validate_RowCount_RAW_DATAMART_CUREATED_BKP_API() throws Exception
		{
			excel= new ExcelLoader(XLPath, "Row_Validation_API+BKP");
			XSSFSheet sh1 = excel.LoadDataFromSheet();
			for(int i=1; i<=sh1.getLastRowNum();i++)
			{
				String DataMart_TenantType = sh1.getRow(i).getCell(1).getStringCellValue();
				String Curated_TenantType = sh1.getRow(i).getCell(3).getStringCellValue();
				String Datamart_Table = sh1.getRow(i).getCell(2).getStringCellValue();
				String Curated_Table = sh1.getRow(i).getCell(4).getStringCellValue();
				
				ExtentReport.setTestName(Curated_TenantType +" - "+ Curated_Table, "Validate the row count of BKP + API table from  DATAMART VS CURATED");
							
				int SF_DataMart=  snowflake.readDB(" select count(*) as TOTAL from \"CURATED_DATAMART\"."+"\""+DataMart_TenantType+"\""+ "."+"\""+Datamart_Table+"\"" + "where ACTIVE_FLAG='Y'");
				int SF_CURATED_API=  snowflake.readDB(" select count(*) as TOTAL from \"DEV_ENT_CURATED\".\"MASTER_DIMENSIONS_BUT\"."+"\""+Curated_Table+"\""+ "where tenant_type='"+Curated_TenantType+"'"+" and Record_Updated_by='API ETL'");
				int SF_CURATED_BKP=  snowflake.readDB(" select count(*) as TOTAL from \"DEV_ENT_CURATED\".\"MASTER_DIMENSIONS_BUT\"."+"\""+Curated_Table+"\""+ "where tenant_type='"+Curated_TenantType+"'"+" and Record_Updated_by='Backup Table'");

				if(SF_DataMart == SF_CURATED_API)
				{
					PrintUtils.logMsg("Row Count of BKP + API Table "+Curated_Table +" is matched for DataMart :: "+ SF_DataMart + " CURATED API :: " + SF_CURATED_API);
				}
				else if(SF_DataMart == SF_CURATED_BKP)
				{
					PrintUtils.logMsg("Row Count of BKP + API Table "+Curated_Table +" is matched for DataMart :: "+ SF_DataMart + " CURATED BKP :: " + SF_CURATED_BKP);
				}
				else 
				{
					PrintUtils.logError("Row Count of BKP + API Table "+Curated_Table+" is not matched for DataMart :: "+ SF_DataMart + " CURATED API:: " + SF_CURATED_API + " CURATED BKP:: " + SF_CURATED_BKP);
				}
				
				ExtentReport.endReport();
			}
			
			sa.assertAll();
			
		}
	 //---------------------------------------------------------------------------------------------------------------------------------

	
	
}
