package ftp;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * 文件名  
 * 20171103_时间戳.dat        ---->  20171103_1.dat.bak
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
 * @author Administrator
 *
 */
public class FileDispatchMain {
	public static void main(String[] args) throws ClassNotFoundException, IOException {

		//每次运行获取当天时间戳中最小的那个文件
		String targetFileName = Utils.getTargetFileName();
		System.out.println(targetFileName);
		
		if(targetFileName == null){
			
		}else{

			//读取文件的内容，解析成list
			List<String> params = Utils.parsedFile(targetFileName);
		    //写入文件

			Utils.writeToTxt(params,targetFileName);
			
			//调shell ，shell中的逻辑包括：1将原始数据进行处理，生成下发数据，2 将用于下发的数据 依照不用的省份分别在96  97 主机上执行下发逻辑
			//昌平
//			Utils.command("sh /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/execute_main_shell.sh "  + targetFileName);
			//亦庄
//			Utils.command("sh /app/jzyx/model_outomatic_put_files/execute_main_shell.sh "  + targetFileName);
			//更改已经处理过的文件名
//			Utils.changeTargetFileName(targetFileName);
			//昌平要加上这句
//			System.out.println("aaabbbb");
		}
		
		
		
	}
}
