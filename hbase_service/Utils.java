package hbase_service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class Utils {

	

	//两年内的时间

	public static List<String> getNearDate(){  
	    List<String> resultList = new ArrayList<String>();  
	    Calendar cal = Calendar.getInstance();  
	    cal.setTime(new Date(System.currentTimeMillis()));
	    int month = cal.get(Calendar.MONTH) + 1;


	        cal.set(Calendar.MONTH, cal.get(Calendar.MONTH)+1); 
	        for(int i=0; i<month + 12; i++){    
	            cal.set(Calendar.MONTH, cal.get(Calendar.MONTH)-1); 
	            resultList.add(String.valueOf(cal.get(Calendar.YEAR)) + (cal.get(Calendar.MONTH)+1 < 10 ? "0" + (cal.get(Calendar.MONTH)+1) : (cal.get(Calendar.MONTH)+1)));  
	        }  
	    
	      
	    return resultList;  
	}   
	
	//读取baseInfo的信息


	public static LinkedHashMap<String,String> field_baseInfo() throws ClassNotFoundException, IOException {
		InputStreamReader fis = new InputStreamReader(Utils.class.getResourceAsStream("field_baseInfo"),"UTF-8");
		String read = null;
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		BufferedReader bf =new BufferedReader(fis);
		while((read = bf.readLine()) != null){
			String[] strs = read.split(",");
			map.put(strs[0], strs[1]);
		}
		bf.close();
		return map;
		
	}
	
	
	//读取terminalChange的信息

	public static List<String> field_terminalChange() throws ClassNotFoundException, IOException {
		InputStreamReader fis = new InputStreamReader(Utils.class.getResourceAsStream("field_terminalChange"),"UTF-8");
		String read = null;
		List<String> list = new ArrayList<String>();
		BufferedReader bf =new BufferedReader(fis);
		while((read = bf.readLine()) != null){
			list.add(read);
		}
		bf.close();
		return list;
		
	}


	//读取terminalChange的信息

	public static Map<String,String> field_marketingSuggestion() throws ClassNotFoundException, IOException {
		InputStreamReader fis = new InputStreamReader(Utils.class.getResourceAsStream("field_marketingSuggestion"),"UTF-8");
		String read = null;
		Map<String, String> map = new HashMap<String, String>();
		BufferedReader bf =new BufferedReader(fis);
		while((read = bf.readLine()) != null){
			String[] strs = read.split(",");
			map.put(strs[0], strs[1]);
		}
		bf.close();
		return map;
		
	}
}
