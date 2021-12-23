package testcases;

import java.util.ArrayList;
import java.util.HashMap;

import org.testng.annotations.*;
import org.testng.asserts.SoftAssert;

import common.Constants;
import common.ExcelLoader;
import common.ExtentReport;
import common.Validator;
import dbdriver.SQLConnector;
import dbdriver.SnowFlakeConnector;

public class BaseClass extends Validator{

	public  static SQLConnector sql=new SQLConnector();
	public  static SnowFlakeConnector snowflake= new SnowFlakeConnector();
	public static ArrayList<String> actualList=new ArrayList<String>();
	public static ArrayList<String> expectedList=new ArrayList<String>();
	public static HashMap<String, String> sqlMap = new HashMap<String, String>();
	public static HashMap<String, String> snowMap = new HashMap<String, String>();
	public ExcelLoader excel=null;
	
	public static String SRCSchema;
	public static String StageSchema;
	public static String TargetSchema;
	public String env ="DEV";
	
	@BeforeSuite
	public void start() 
	{
		switch (env)
		{
		case "DEV": 
			SRCSchema=Constants.DevSchema.DEV_ENT_RAW.toString();
			StageSchema= Constants.DevSchema.CURATED_DATAMART.toString();
			TargetSchema=Constants.DevSchema.DEV_ENT_CURATED.toString();
			break;

		default: System.out.println("Plz Set correct Env name ");
			break;
		}
	}
	
	@BeforeClass
	public void beforeClass() 
	{
		String className = this.getClass().getSimpleName();
		ExtentReport.startReport(className);
		//sql =new SQLConnector();
		//sql.connectToDB();
		
		snowflake = new SnowFlakeConnector();
		snowflake.connectToDB();
	}
	@AfterClass
	public void afterClass() 
	{
		//sql.closeDBConnection();
		snowflake.closeDBConnection();
		ExtentReport.endReport();
	}
	
	@BeforeMethod
	public void beforeMethod() 
	{
		actualList.clear();
		expectedList.clear();
		sqlMap.clear();
		snowMap.clear();
		sa= new SoftAssert();
	}
}
