package common;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.relevantcodes.extentreports.ExtentReports;
import com.relevantcodes.extentreports.ExtentTest;
import com.relevantcodes.extentreports.LogStatus;

public class ExtentReport {

	private static ExtentTest test;
	private static ExtentReports extent;


	public static void startReport(String name)
	{
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd-MM-YYYY");  
		LocalDateTime now = LocalDateTime.now();
		String dateFolder =dtf.format(now);

		DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("dd-MM-YYYY HH:mm");  
		LocalDateTime now1 = LocalDateTime.now();
		String DateName =new String(dtf1.format(now1));
		DateName = DateName.replace(" ", "-") ;
		DateName = DateName.replace(":", "-") ;

		extent = new ExtentReports("./ExtentReport/"+dateFolder+"/DF_Report_"+name+"_"+DateName.trim()+".html", true);
		extent.loadConfig(new File("./extent-config.xml"));
	}

	public static void setTestName(String testCaseName, String testDescription ) 
	{
		test = extent.startTest(testCaseName, testDescription);
	}

	public static void testStep(String desc, String status) 
	{
		if (status.toUpperCase().equals("PASS"))
		{
			test.log(LogStatus.PASS, desc);
		}
		else if (status.toUpperCase().equals("FAIL"))
		{
			test.log(LogStatus.FAIL, desc );
		}
		else if (status.toUpperCase().equals("INFO"))
		{
			test.log(LogStatus.INFO, desc);
		}
	}

	public static void endReport() 
	{
		extent.endTest(test);
		extent.flush();
	}

}
