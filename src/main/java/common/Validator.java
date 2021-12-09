package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.asserts.SoftAssert;

public class Validator {
	
	
	public static SoftAssert sa;
	
	public static void compareList(List<String> actualList,List<String> expectedList) 
	{
		if(actualList.equals(expectedList))
		{
			PrintUtils.logMsg("List matched");
		}else 
		{
			PrintUtils.logError("List Not Matched");
		}
	}
	
	public static void compareSet(Set<Object> actualList,Set<Object> expectedList) 
	{
		if(actualList.equals(expectedList))
		{
			PrintUtils.logMsg("Set matched");
		}else 
		{
			PrintUtils.logError("Set Not Matched");
		}
	}
	
	public static void compareSet(HashMap<String,String> sqlMap,HashMap<String,String> snowMap) 
	{
		sqlMap.forEach((SQLkey,SQLvalue)->
		{ 
			String snowValue =  snowMap.get(SQLkey.toUpperCase()); 
			if(SQLvalue.equals(snowValue))
			{
				PrintUtils.logMsg("Row count matched for table "+SQLkey+" | "+SQLvalue+" | "+snowValue);
			}
			else
			{
				PrintUtils.logError("Row count mismatch for table "+SQLkey+" | "+SQLvalue+" | "+snowValue); 
			}
		});
	}
	
	 public static void checkMatch(ArrayList<String> arraylist1, ArrayList<String> arraylist2 ) {
	        HashSet<String> hs1 = new HashSet<String>();
	        HashSet<String> hs2 = new HashSet<String>();
	        for (String match : arraylist2 ) {
	            hs1.add(match);
	        }
	        for (String match : arraylist1) {
	            hs2.add(match);
	        }
	        for (String match : hs2) {
	            boolean b = hs1.add(match);
	            if (b == false) {
	                @SuppressWarnings("unused")
					String matchingFields = match;
	                //System.out.println("-----------------matching Elements--------------------- =  " + 
	                      //  matchingFields);

	            } else {
	                String nonMatchingFields = match;
	                System.err.println("-----------------Not matching elements---------------------  =  " + 
	                        nonMatchingFields);
	            }
	        }
	 }
}
