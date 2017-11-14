package ftp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.spi.DirStateFactory.Result;

public class Utils {

	
	
	public static LinkedHashMap<String,String> get_Dir() throws ClassNotFoundException, IOException {
		InputStreamReader fis = new InputStreamReader(Utils.class.getResourceAsStream("config"),"utf-8");
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
	
	public static String getTargetFileName() throws ClassNotFoundException, IOException{
		LinkedHashMap< String, String> ftpDir_map = Utils.get_Dir();
		String ftpDir = ftpDir_map.get("ftpDir");
//		System.out.println(ftpDir);
		File dir = new File(ftpDir);
		File[] files = dir.listFiles();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		List<Long> list = new ArrayList<Long>();
		String currentDay = sdf.format(System.currentTimeMillis());
		for(File file : files){ 
			if(file.getName().contains(".bak")){
				continue;
			}else if(!file.getName().contains(currentDay)){
				continue;
			}else{
				//20171103_5.dat
				Long index = Long.valueOf(file.getName().split("_")[1].split("\\.")[0]);
				list.add(index);
			}
		}
		Collections.sort(list);
		String targetFileName = "";
		if(list.size() == 0){
			return null;
		}else{
			targetFileName = currentDay +"_"+list.get(0) +".dat";
		}
		
		//返回最小的文件的文件名
		

		
		return targetFileName;
	}
	
	
	
	
	
	public static void changeTargetFileName(String targetFileName) throws ClassNotFoundException, IOException{
		LinkedHashMap< String, String> ftpDir_map = Utils.get_Dir();
		String ftpDir = ftpDir_map.get("ftpDir");
		String resultFile = ftpDir +"//" + targetFileName;
		File oldFileName = new File(resultFile);
		
		//获取根路径
		String parentPath = oldFileName.getParent();
		String filename = oldFileName.getName();
		File newFileName = new File(parentPath + File.separator + filename  +".bak");

		oldFileName.renameTo(newFileName);
	}
	
	
	//读取文件内容并解析出参数并形成list
	public static List<String> parsedFile (String targetFileName) throws ClassNotFoundException, IOException{

		List<String> list = new ArrayList<String>();
		LinkedHashMap< String, String> ftpDir_map = Utils.get_Dir();
		String ftpDir = ftpDir_map.get("ftpDir");
		
		File file = new File(ftpDir + File.separator + targetFileName);  
	    BufferedReader reader = null;  
	        try {  
	            Map<String, String> paramsMap = new HashMap<String,String>();
	            reader = new BufferedReader(new FileReader(file));  
	            String tempString = null;  
	            while ((tempString = reader.readLine()) != null) {
	            	String tempStr = tempString.replace("{", "").replace("}", "").replace("\"", "").replace(";", "");
		            	if(!tempStr.contains("union")){
		            		
		            		list = Utils.getMyResult(tempStr);
		            		
		            	}else{
		            		
		            		//对传过来的字符串进行处理
		            		String temp_str = "";
		            		String preferStr = tempStr.split("stepSql:")[0];
		            		String[] units = tempStr.split("stepSql:")[1].split("union");
		            		
		            		List<String> list_sqls = new ArrayList<String>();
		            		List<String> list_middle = new ArrayList<String>();
		            		List<String> list_nosqls = new ArrayList<String>();
		            		String sql_strs = "";
		            		String model_strs = "";
		            		String prvnce_strs = "";
		            		
		            		
		            		for(String unit : units){
		            			temp_str += preferStr+"stepSql:"+unit + "union";
		            		}
		            				
		            		temp_str = temp_str.substring(0,temp_str.length() - 5);
		            				
		            		
		            		
//		            		System.out.println("aaaaaaaaaaaaaaaaaa" + temp_str);
		            			

		            		String[] sqlStrs = temp_str.split("union");
		            		//获取没有sql没有省份没有模型id的那些元素
		            		list_middle = Utils.getMyResult(sqlStrs[0]);
		            		for(String str : list_middle){
		            			   
//		            			if(!str.contains("sqlcontant<->") && !str.contains("modelid<->") && !str.contains("prvnceids<->")){
//		            				
//		            			}
		            			if(str.contains("dayid<->") || str.contains("modeltype<->") || str.contains("campaintype<->")|| str.contains("markingid<->")){
		            				list_nosqls.add(str);
		            			}
		            		}
		            		
		            		
		            		for(String sqlStr : sqlStrs){
		            			list_sqls = Utils.getMyResult(sqlStr);
		            			for(String str : list_sqls){
		            				if(str.contains("sqlcontant<->")){
		            					sql_strs += str.split("sqlcontant<->")[1] + "#";
		            				}
		            				if(str.contains("modelid<->")){
		            					model_strs += str.split("modelid<->")[1] + ";";
		            				}
		            				if(str.contains("prvnceids<->")){
		            					prvnce_strs += str.split("prvnceids<->")[1] + ";";
		            				}
		            			}
		            		}
		            		sql_strs = sql_strs.substring(0,sql_strs.length() - 1);
		            		model_strs = model_strs.substring(0,model_strs.length() - 1);
		            		prvnce_strs = prvnce_strs.substring(0,prvnce_strs.length() - 1);
		            		sql_strs = "sqlcontant<->" + sql_strs;
		            		model_strs = "modelid<->" + model_strs;
		            		prvnce_strs = "prvnceids<->" + prvnce_strs;
		            		list_nosqls.add(model_strs);
		            		list_nosqls.add(prvnce_strs);
		            		list_nosqls.add(sql_strs);
		            		list = list_nosqls;
		            	}
	            	}  
	            reader.close();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        } finally {  
	            if (reader != null) {  
	                try {  
	                    reader.close();  
	                } catch (IOException e1) {  
	                }  
	            }  
	        }
	        return list;
		
		
	}
	
	
	
	
	
	
	
	//写文件
	
	    public static void writeToTxt(List<String> list,String filename) throws ClassNotFoundException, IOException{
			LinkedHashMap< String, String> Dir_map = Utils.get_Dir();
			String toDir = Dir_map.get("toDir");
	        Iterator iterator = list.iterator();
	        File file = new File(toDir + File.separator + filename);
	        FileWriter fw = null;
	        BufferedWriter writer = null;
	        try {
	            fw = new FileWriter(file);
	            writer = new BufferedWriter(fw);
	            while(iterator.hasNext()){
	                writer.write(iterator.next().toString());
	                writer.newLine();//换行
	            }
	            writer.flush();
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        }catch (IOException e) {
	            e.printStackTrace();
	        }finally{
	            try {
	                writer.close();
	                fw.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
	    public static void command(String command){  
	        try  
	        {  
	            Process process=new ProcessBuilder(Arrays.asList(command.split(" "))).start();  
	            //标准输入流  
	            BufferedReader result= new BufferedReader(new InputStreamReader(process.getInputStream()));  
	            String s=result.readLine();  
	            while(s!=null){  
	                s=result.readLine();  
	            }  
	            //标准错误输入流  
	            BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));  
	            s=error.readLine();  
	            while(s!=null){  
	                s=error.readLine();  
	            }  
	        } catch (Exception e)  
	        {  
	            //纠正  
	            if(!command.startsWith("CMD /C")){  
	                command("CMD /C "+command);  
	            }else{  
	                throw new RuntimeException(e.getMessage());  
	            }  
	        }  
	    }  
	    
	    //解析单条的逻辑  传入一个字符串  传出的是个list
	    public static  List<String>  getMyResult(String tempStr){
	    	
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			List<String> list = new ArrayList<String>();
          if(tempStr.contains("AS CYCLE_NBR ,")){
	            //获取账期
	            list.add("dayid<->"+sdf.format(System.currentTimeMillis()));
	            //取数类型
	            String takemodelType = tempStr.split("takeModeId")[1].split(",")[0].replace(":", "");
	            list.add("modeltype<->"+takemodelType);
	            //活动类型
	            String campaintype = tempStr.split("campaignTypeId")[1].split(",")[0].replace(":", "");
	            list.add("campaintype<->"+campaintype);
	            
	            
	            
	            //获取活动id和模型id
	            String modelId = "";
	            Pattern pattern = Pattern.compile("[^0-9]");
	            String[] strs = tempStr.split(",");
	            for(String str : strs){
	            	//活动Id
	            	if(str.contains("marketing_code")){
	            		Matcher matcher = pattern.matcher(str);
	                    String actionId = matcher.replaceAll("");
	                    list.add("markingid<->"+actionId);
	            	}
	            	//模型Id
	            	if(str.contains("sale_busi_type")){
	            		Matcher matcher = pattern.matcher(str);
	                    modelId = matcher.replaceAll("");
	                    list.add("modelid<->"+modelId);
	            	}
	            }
	            
	            //获取可变字段
	
	        	 String qualities = tempStr.split("AS CYCLE_NBR ,")[1].split("from")[0];
	             int quelities_num = qualities.split(",").length;
	             String zero_str = "";
	             for (int i = 0 ; i < 50 - quelities_num ; i++){
	             	zero_str += "\"\",";
	             }
	             
	             String praced_qualities = qualities +","+zero_str;
	             String praced_qualities_1 = praced_qualities.substring(0,praced_qualities.length() -1);
	
	           
	            
	            //获取where条件
	            //where 1=1 and STD_PRVNCE_CD=861 and in_nets_duratio>150;
	            String whereConfig = tempStr.split("where")[1];
	            String privncId = "";
	            if(!whereConfig.contains("std_prvnce_cd")){
	            	 //获取省份 _当没有省份条件的
	            	privncId = "811,812,813,814,815,821,822,823,831,832,833,834,835,836,837,841,842,843,844,845,846,850,851,852,853,854,861,862,863,864,865";
	            }
	            String[] configs = whereConfig.split("and");
	            String strBuild = "";
	            for(String config : configs){
	            	if(config.contains("std_prvnce_cd=")){
	            		privncId = config.split("std_prvnce_cd=")[1].trim().replace("\'", "");
	            	}else if(config.contains("std_prvnce_cd  in")){
	            		privncId = config.split("std_prvnce_cd  in")[1].trim().replace("(", "").replace(")", "").replace("\'", "");
	            	}else if(config.contains("1=1") ||config.contains("STD_PRVNCE_CD")){
	            		continue;
	            	}else{
	            		strBuild +=  "and  " + config; 
	            	}
	            }
	            
	            
	            
	            list.add("prvnceids<->"+modelId+"<#>"+privncId);
	            //亦庄的sql
//	            list.add("sqlcontant<->"+"insert overwrite table tm_mktag_sales_user_toCchnnel_day partition(day_id=day01,prvnce_id=prov01,marketing_code=mark01,model_id="+modelId+") select std_latn_cd,accs_nbr, '1' marketing_batch,"+praced_qualities_1+" from ta_mktag_total_day where day_id=day02 and prvnce_id=prov01 " + strBuild);
	            //昌平的sql
	            list.add("sqlcontant<->"+"insert overwrite table tm_mktag_sales_user_toCchnnel_day partition(day_id=day01,prvnce_id=prov01,marketing_code=mark01,model_id="+modelId+") select std_latn_cd,accs_nbr, '1' marketing_batch,"+praced_qualities_1+" from ta_mktag_total_month where std_prvnce_cd=prov01 " + strBuild);
	            list.add("gameover");
          }else{
        	  //获取账期
	            list.add("dayid<->"+sdf.format(System.currentTimeMillis()));
	            //取数类型
	            String takemodelType = tempStr.split("takeModeId")[1].split(",")[0].replace(":", "");
	            list.add("modeltype<->"+takemodelType);
	            //活动类型
	            String campaintype = tempStr.split("campaignTypeId")[1].split(",")[0].replace(":", "");
	            list.add("campaintype<->"+campaintype);
	            String mktManageCode = tempStr.split("mktManageCode")[1].split(",")[0].replace(":", "");
	            list.add("mktManageCode<->"+mktManageCode);
	            list.add("gameover");
          }
	    	
	    	
			return list;
	    	
	    }

}
