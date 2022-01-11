package testcases.ServiceTitan;

import java.util.ArrayList;
import java.util.List;
import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime;   
public class Test {

	//	
	public static void main(String[] args) {    
		List<String> mlist = new ArrayList<>();
		List<String> dlist = new ArrayList<>();

		String manual ="010025,010051,010071,010081,010088,010095,010118,010121,010135,010137,010144,010261,010341,010355,010373,010377,010412,010436,010474,010476,010529,010556,010568,010575,010618,010626,010634,010677,010679,010753,010757,010791,010794,010801,010821,010825,010827,010830,010837,010840,010844,010867,010882,010887,010889,010891,010909,010920,010936,010941,011007,011063,011066,011073,011100,011126,011156,011160,011168,011170,011178,011179,011201,011228,011232,011239,011321,011351,011387,011401,011425,011427,011429,011438,011453,011552,011586,011600,011601,011608,011666,011692,011768,011769,011774,011775,011791,011818,011820,011826,011832,011835,011836,011846,011847,011852,011879,011932,011945,011949,011991,011994,012003,012026,012029,012098,012099,012148,012154,012159,012160,012166,012167,012181,012189,012533,012557,012583,012609,012610,012613,012614,012652,012671,012699,012707,012710,012712,012714,012723,012726,012727,012775,012786,013812,013816,013829,013830,013861,013944,013950,013962,014005,014019,014021,014126,014140,014147,014153,014164,014170,014171,014196,014309,014310,014319,014331,014351,014366,014390,014405,014411,014417,014432,014474,014483,014485,014513,014532,014535,014562,014574,014585,014592,014616,014621,014622,014627,014638,014640,014645,014647,014661,014693,014746,014751,014779,014784,014785,014795,014799,014822,014827,014841,014850,014880,014892,014894,014897,014901,014902,014909,014910,014912,014919,014927,014935,014936,014960,014966,014972,014978,015037,015039,015045,015053,015059,015062,015086,015105,015109,015119,015128,015131,015138,015143,015162,015193,015202,015206,015208,015227,015235,015236,015242,015259,015266,015276,015288,015291,015294,015303,015320,015334,015337,015344,015347,015432,015456,015733,015868,015894,015901,015917,015940,016015,016016,016018,016044,016168,016172,016174,016216,016229,016261,016278,016287,016304,016314,016351,016370,016405,016419,016444,016454,016491,016558,016574,016613,016620,016621,016641,016658,016702,016706,016707,016735,016749,016755,016785,031935,10705,12110,14515,14630,14698,14712,14742,14783,14832,15002,15081,15089,15108,15137,15145,15170,15299,15306,15333,15481,15482,15659,15665,15687,15689,15701,15705,15730,15737,15738,15739,15744,15746,15761,15791,15793,15801,15816,15830,15855,15876,15877,15886,15887,15888,15895,15902,15938,15942,16042,16045,16047,16105,16169,16235,16282,16286,16303,16305,16372,16457,16481,16493,16538,16540,16541,16555,16577,16578,16583,16607,16614,16650,16651,16655,16656,16728,16763,16777,16804,16820,16821";
		String demo ="10025,010051,010071,010081,010088,010095,010118,010121,010135,010137,010144,010261,010341,010355,010373,010377,010412,010436,010474,010476,010529,010556,010568,010575,010618,010626,010634,010677,010679,10705,010753,010757,010791,010794,010801,010821,010825,010827,010830,010837,010840,010844,010867,010882,010887,010889,010891,010909,010920,010936,010941,011007,011063,011066,011073,011100,011126,011156,011160,011168,011178,011179,011201,011228,011232,011239,011321,011351,011387,11391,011401,011425,011427,011429,011438,011453,011552,011586,011600,011601,011608,011666,011692,011767,011768,011769,011774,011775,011791,011818,011820,011826,011832,011835,011836,011846,011847,011852,011879,011932,011945,011949,011991,011994,012003,012026,012029,012098,012099,12110,012148,012154,012159,012160,012166,012167,012181,012189,012533,012557,012583,012609,012610,012613,012614,012652,012671,012699,012707,012710,012712,012714,012723,012726,012727,012775,012786,013812,013816,013829,013830,013861,013944,013950,013962,014005,014019,014021,014126,014140,014147,014153,014154,014164,014170,014171,014196,014309,014310,014319,014331,014351,014366,014390,014405,014411,014417,014432,014474,014483,014485,014513,14515,014532,014535,014562,014574,014585,014592,014616,014621,014622,014627,14630,014638,014640,014645,014647,014661,014693,14698,14712,14742,014746,014751,014779,14783,014784,014785,014795,014799,014822,014827,14832,014841,014850,014880,014892,014894,014897,014901,014902,014909,014910,014912,014919,014927,014935,014936,014960,014964,014966,014972,014978,15002,015037,015039,015045,015053,015059,015062,15081,015086,15089,015105,15108,015109,015119,015128,015131,15137,015138,015143,15145,015162,15170,015193,015202,015206,015208,015227,015235,015236,015242,015259,015266,015276,015288,015291,015294,15299,015303,15306,015320,15333,015334,015337,015344,015347,015432,015456,15470,15481,15482,15659,15663,15665,15687,15689,15701,15705,15730,015733,15737,15738,15739,15744,15746,15761,15791,15793,15801,15816,15830,15855,015868,15876,15877,15886,15887,15888,015894,15895,015901,15902,015917,15938,015940,15942,016015,016016,016018,16042,016044,16045,16047,16105,016168,16169,016172,016174,016216,016229,16235,016261,016278,16282,16286,016287,16303,016304,16305,016314,016351,016370,16372,016405,016419,016444,016454,16455,16457,16481,016491,16493,16538,16540,16541,16555,016558,016574,16577,16578,16581,16583,16607,016613,16614,016620,016621,016641,16650,16651,16655,16656,016658,016702,016706,016707,16728,016735,016749,016755,16763,16777,016785,16796,16804,16820,16821,031935";
		
		
		String value=null;
		for(int i =0; i<manual.split(",").length;i++)
		{
			value = manual.split(",")[i];
			mlist.add(value);
			
		}

		System.out.println(mlist);
		System.out.println("Manual size:: "+mlist.size());
		
		for(int i =0; i<demo.split(",").length;i++)
		{
			value = demo.split(",")[i];
				
			dlist.add(value);
			
		}

		System.out.println(dlist);
		System.out.println("Demo size:: "+dlist.size());
		
		for(int k =0; k<mlist.size();k++)
		{
			if(!dlist.contains(mlist.get(k)))
			{
				System.err.println("Mlist missing"+mlist.get(k));
			}
		}
		
		for(int k =0; k<dlist.size();k++)
		{
			if(!mlist.contains(dlist.get(k)))
			{
				System.err.println("dlist missing"+dlist.get(k));
			}
		}
	}
}