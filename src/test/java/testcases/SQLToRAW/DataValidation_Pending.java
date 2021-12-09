package testcases.SQLToRAW;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.testng.annotations.Test;

import common.ExtentReport;
import common.PrintUtils;
import testcases.BaseClass;


public class DataValidation_Pending extends BaseClass{
	
	
	@Test
	public void test() throws SQLException 
	{
		ExtentReport.setTestName("","");
		ResultSet rs= sql.readDBAndReturnResultSet("use St_Backup_FourServicePros SELECT A.Name AS Src_Table , SUM(B.rows) AS Src_RowCount FROM sys.objects A INNER JOIN sys.partitions B ON A.object_id = B.object_id WHERE A.type = 'U' and (SCHEMA_NAME(A.schema_id)) = 'fourservicepros' GROUP BY A.schema_id, A.Name ORDER BY A.Name");
		PrintUtils.logMsg("Getting size");
		while (rs.next())
		{
			PrintUtils.logMsg("value is "+rs.getString(1));
		}
	}

}
