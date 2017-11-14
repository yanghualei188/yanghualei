package ftp;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
public class Test {
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
//		try { 
//	        FileInputStream in=new FileInputStream(new File("D:/test.txt")); 
//	        boolean flag = uploadFile("127.0.0.1", 21, "test", "test", "D:/test", "test.txt", in); 
//	        System.out.println(flag); 
//	    } catch (FileNotFoundException e) { 
//	        e.printStackTrace(); 
//	    } 

		
//		
//		FTPClient ftpClient = new FTPClient(); 
//        FileInputStream fis = null; 
//
//        try { 
//            ftpClient.connect("10.139.0.130"); 
//            ftpClient.login("odsftp", "AhvRab5!"); 
//
//            File srcFile = new File("/home/ctgmkt/jzyx/yhl/aaa.txt"); 
//            fis = new FileInputStream(srcFile); 
//            //设置上传目录 
//            ftpClient.changeWorkingDirectory("/home/ctgmkt/jzyx/yhl/ftp"); 
//            ftpClient.setBufferSize(1024); 
//            ftpClient.setControlEncoding("UTF-8"); 
//            //设置文件类型（二进制） 
//            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE); 
//            ftpClient.storeFile("3.gif", fis); 
//        } catch (IOException e) { 
//            e.printStackTrace(); 
//            throw new RuntimeException("FTP客户端出错！", e); 
//        } finally { 
//            IOUtils.closeQuietly(fis); 
//            try { 
//                ftpClient.disconnect(); 
//            } catch (IOException e) { 
//                e.printStackTrace(); 
//                throw new RuntimeException("关闭FTP连接发生异常！", e); 
//            } 
//        } 
		
		
//		Ftp f = new Ftp();
//		f.setIpAddr("10.139.0.130");
////		f.setPath(path);
//		f.setPort(21);
//		f.setPwd("AhvRab5!");
//		f.setUserName("odsftp");
//		FtpUtil.connectFtp(f);
//        File file = new File("/home/ctgmkt/jzyx/yhl/aaa.txt");  
//        FtpUtil.upload(file);//把文件上传在ftp上
//		System.out.println("ok");
		
		

//		
//		String phoneString = "201701122024 as  marketing_code";
//        // 提取数字
//        // 1
//        Pattern pattern = Pattern.compile("[^0-9]");
//        Matcher matcher = pattern.matcher(phoneString);
//        String all = matcher.replaceAll("");
//        System.out.println(all);
//        // 2
//        Pattern.compile("[^0-9]").matcher(phoneString).replaceAll("");
		
//		System.out.println("aaaaaaaaaaaaaaaaabbbbbbbbbbbbbbb");
		
		List<String> list = new ArrayList<String>();
//		String a = "aa";
//		list.add(a);
//		list.add("bb");
//		list.add("cc");
//		for(String e :list){
//			System.out.println(e);
//		}
		String aString = "sh /app/jzyx/model_outomatic_put_files/execute_main_shell.sh   /app/jzyx/model_outomatic_put_files/parameter_files/";
		
		
		for(String eString : aString.split(" ")){
			System.out.println(eString);
		}
        	
        
        
	}
}
