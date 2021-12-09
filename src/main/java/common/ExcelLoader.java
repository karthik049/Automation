package common;

import java.io.File;
import java.io.FileInputStream;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelLoader {

	private String workbook=null;
	private String Sheet=null;
	
	public File file =null;
	public FileInputStream fis =null;
	public XSSFWorkbook wb=null;
	public XSSFSheet sh =null;
			
	public ExcelLoader(String workbookPath, String SheetName)
	{
		this.workbook=workbookPath;
		this.Sheet=SheetName;
		//PrintUtils.logMsg("Workbook and Sheet used :: "+ workbook + Sheet);		
	}
	
	public XSSFSheet LoadDataFromSheet() throws Exception 
	{
		file = new File(workbook);
		fis = new FileInputStream(file);
		wb = new XSSFWorkbook(fis);
		sh= wb.getSheet(Sheet);
		return sh;
	}
}
