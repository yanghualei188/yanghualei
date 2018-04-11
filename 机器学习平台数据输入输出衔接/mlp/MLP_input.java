package mlp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

public class MLP_input {
	public static void main(String[] args) throws Exception {
		/**
		 * 回的逻辑
		 * 定时的    去get文件  并load后 执行hive脚本 并 放入到指定的hive表（全量覆盖）
		 */
		
		//readed_flies
//		File dir_path = new File("/home/mlp_user/mlp_out");
//		if(dir_path.isDirectory()){
//			File[] files = dir_path.listFiles();
//			String lastFileName = files[files.length - 1].getName();
//			System.out.println(lastFileName);
//			String str = "";
//			File file = new File("/home/mlp_user/yhl/readed_flies");
//			BufferedReader br = new BufferedReader(new FileReader(file));
//		    HashSet<String> filenameSet = new HashSet<String>();
//			while((str = br.readLine()) != null){
//				filenameSet.add(str);
//
//			}
//			if(!filenameSet.contains(lastFileName)){
//				
//			   
//			}
//            br.close();
//            
//			//还要往readed_files中写入新的文件 追加写入
//		    FileWriter fw = new FileWriter("/home/mlp_user/yhl/readed_flies",true);
//		    fw.write(lastFileName);
//		    fw.flush();
//		    fw.close();
		    
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			
		
		
		
		    RemoteExecuteCommand rec=new RemoteExecuteCommand("10.140.16.205", "ctgmkt","ctgmkt@123"); 
		    //执行205上ftp到ftp服务器 get
		    rec.executeSuccess("expect /home/ctgmkt/yhl/ftp_get.exp *" + sdf.format(new Date())+"*");
		    
		    //再鉴别
		    rec.executeSuccess("ls /tmp/mlp_in/ | grep "+sdf.format(new Date())+" >/home/ctgmkt/yhl/mlp_in_filename.txt");
//		    //执行hive load数据   (这个还没有检验)
		    rec.executeSuccess("hive -hiveconf filename=${gawk '{ print $0 }' /home/ctgmkt/yhl/mlp_in_filename.txt }  -f /home/ctgmkt/yhl/hive_load.hql");
 
		
		}

		
		
	}

