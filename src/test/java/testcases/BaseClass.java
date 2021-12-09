package testcases;

import java.util.ArrayList;
import java.util.HashMap;

import org.testng.annotations.*;
import org.testng.asserts.SoftAssert;

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
	@BeforeSuite
	public void beforeSuite() 
	{
		ExtentReport.startReport();
		//sql =new SQLConnector();
		sql.connectToDB();
		
		//snowflake = new SnowFlakeConnector();
		snowflake.connectToDB();
	}
	@AfterSuite
	public void afterSuite() 
	{
		sql.closeDBConnection();
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
