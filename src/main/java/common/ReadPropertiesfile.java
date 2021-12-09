package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ReadPropertiesfile {

	public static Properties prop;  
	
	public ReadPropertiesfile() 
	{
		try {
			File file = new File("./src/main/resources/DBDetails/config.properties");
			FileInputStream fis = new FileInputStream(file);
			prop=new Properties();  
			prop.load(fis);
		} 
		catch (IOException e) 
		{
			PrintUtils.logError("FileNotFoundException in ReadPropertiesfile() method :: "+e.getMessage());
		}  
	}

	public static String getPropData(String key)
	{
		String value =prop.get(key).toString();
		return value;
	}
	
	/*
	 * public static void main(String[] args) {
	 * 
	 * 
	 * ReadPropertiesfile obj= new ReadPropertiesfile();
	 * System.out.println(obj.getPropData("SQL_PWD")); }
	 */


}
