package mlp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashSet;

public class MLP_output {
	public static void main(String[] args) throws Exception {
		/**当有文件的时候，去遍历
		 * 再去读已经处理过的文件
		 * 当有未处理过得文件时，就scp 到 205 并远程登陆到205 后 load 后去执行hive程序 格式化  
		 * 并远程登陆205上的79ftp 把文件上传到79ftp上
		 * 
		 * 
		 * 回的逻辑
		 * 定时的    去get文件  并load后 执行hive脚本 并 放入到指定的hive表（全量覆盖）
		 */
		
		//readed_flies
		File dir_path = new File("/home/mlp_user/mlp_out");
		if(dir_path.isDirectory()){
			File[] files = dir_path.listFiles();
			String lastFileName = files[files.length - 1].getName();
			System.out.println(lastFileName);
			String str = "";
			File file = new File("/home/mlp_user/yhl/readed_flies");
			BufferedReader br = new BufferedReader(new FileReader(file));
		    HashSet<String> filenameSet = new HashSet<String>();
			while((str = br.readLine()) != null){
				filenameSet.add(str);
			}
			if(!filenameSet.contains(lastFileName)){
				//scp 到 205 
				OSExecuter.command("expect /home/mlp_user/yhl/scp.exp 10.140.16.205 ctgmkt ctgmkt@123 /home/mlp_user/mlp_out/"+lastFileName+" /tmp/mlp_out/"+lastFileName);
				//并远程登陆205上的79ftp 把文件上传到79ftp上
			    RemoteExecuteCommand rec=new RemoteExecuteCommand("10.140.16.205", "ctgmkt","ctgmkt@123"); 
			    //执行本地脚本处理逻辑成7+50
			    rec.executeSuccess("gawk -f /home/ctgmkt/yhl/etl.gawk /tmp/mlp_out/"+lastFileName+" 1>/tmp/mlp_out/"+lastFileName + "_ed");
			    rec.executeSuccess("sed -i -f /home/ctgmkt/yhl/etl.sed /tmp/mlp_out/" + lastFileName + "_ed");
			    //执行205上ftp到ftp服务器
			    rec.executeSuccess("expect /home/ctgmkt/yhl/ftp_put.exp " + lastFileName + "_ed");
			}
            br.close();
			//还要往readed_files中写入新的文件 追加写入
		    FileWriter fw = new FileWriter("/home/mlp_user/yhl/readed_flies",true);
		    fw.write(lastFileName);
		    fw.flush();
		    fw.close();
 
		
		}

		
		
	}
}
