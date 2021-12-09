package testcases.RAW_DATAMART_CURATED;

import java.util.ArrayList;
import java.util.List;
import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime;   
public class Test {

	
	public static void main(String[] args) {    
	
	
		DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("dd-MM-YYYY HH:mm");  
		LocalDateTime now1 = LocalDateTime.now();
		String DateName =new String(dtf1.format(now1));
		
		DateName = DateName.replace(" ", "-") ;
		DateName = DateName.replace(":", "-") ;
		System.out.println(DateName);
		
	}
}
