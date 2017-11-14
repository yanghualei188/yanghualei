package ftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

public class FtpUtil {

	 private static Logger logger=Logger.getLogger(FtpUtil.class);
	 
	 private static FTPClient ftp;
	 
	 /**
	  * 获取ftp连接
	  * @param f
	  * @return
	  * @throws Exception
	  */
	 public static boolean connectFtp(Ftp f) throws Exception{
	     ftp=new FTPClient();
	     boolean flag=false;
	     int reply;
	     if (f.getPort()==null) {
	         ftp.connect(f.getIpAddr(),21);
	     }else{
	         ftp.connect(f.getIpAddr(),f.getPort());
	     }
	     ftp.login(f.getUserName(), f.getPwd());
	     ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
	     reply = ftp.getReplyCode();      
	     if (!FTPReply.isPositiveCompletion(reply)) {      
	           ftp.disconnect();      
	           return flag;      
	     }      
	     ftp.changeWorkingDirectory(f.getPath());      
	     flag = true;      
	     return flag;
	 }
	 
	 /**
	  * 关闭ftp连接
	  */
	 public static void closeFtp(){
	     if (ftp!=null && ftp.isConnected()) {
	         try {
	             ftp.logout();
	             ftp.disconnect();
	         } catch (IOException e) {
	             e.printStackTrace();
	         }
	     }
	 }
	 
	 /**
	  * ftp上传文件
	  * @param f
	  * @throws Exception
	  */
	 public static void upload(File f) throws Exception{
	     if (f.isDirectory()) {
	         ftp.makeDirectory(f.getName());
	         ftp.changeWorkingDirectory(f.getName());
	         String[] files=f.list();
	         for(String fstr : files){
	             File file1=new File(f.getPath()+"/"+fstr);
	             if (file1.isDirectory()) {
	                 upload(file1);
	                 ftp.changeToParentDirectory();
	             }else{
	                 File file2=new File(f.getPath()+"/"+fstr);
	                 FileInputStream input=new FileInputStream(file2);
	                 ftp.storeFile(file2.getName(),input);
	                 input.close();
	             }
	         }
	     }else{
	         File file2=new File(f.getPath());
	         FileInputStream input=new FileInputStream(file2);
	         ftp.storeFile(file2.getName(),input);
	         input.close();
	     }
	 }
	 
	 /**
	  * 下载链接配置
	  * @param f
	  * @param localBaseDir 本地目录
	  * @param remoteBaseDir 远程目录
	  * @throws Exception
	  */
	 public static void startDown(Ftp f,String localBaseDir,String remoteBaseDir ) throws Exception{
	     if (FtpUtil.connectFtp(f)) {
	         
	          try { 
	              FTPFile[] files = null; 
	              boolean changedir = ftp.changeWorkingDirectory(remoteBaseDir); 
	              if (changedir) { 
	                  ftp.setControlEncoding("GBK"); 
	                  files = ftp.listFiles(); 
	                  for (int i = 0; i < files.length; i++) { 
	                      try{ 
	                          downloadFile(files[i], localBaseDir, remoteBaseDir); 
	                      }catch(Exception e){ 
	                          logger.error(e); 
	                          logger.error("<"+files[i].getName()+">下载失败"); 
	                      } 
	                  } 
	              } 
	          } catch (Exception e) { 
	              logger.error(e); 
	              logger.error("下载过程中出现异常"); 
	          } 
	      }else{
	          logger.error("链接失败！");
	      }
	      
	  }
	  
	  
	  /** 
	   * 
	   * 下载FTP文件 
	   * 当你需要下载FTP文件的时候，调用此方法 
	   * 根据<b>获取的文件名，本地地址，远程地址</b>进行下载 
	   * 
	   * @param ftpFile 
	   * @param relativeLocalPath 
	   * @param relativeRemotePath 
	   */ 
	  private  static void downloadFile(FTPFile ftpFile, String relativeLocalPath,String relativeRemotePath) { 
	      if (ftpFile.isFile()) {
	          if (ftpFile.getName().indexOf("?") == -1) { 
	              OutputStream outputStream = null; 
	              try { 
	                  File locaFile= new File(relativeLocalPath+ ftpFile.getName()); 
	                  //判断文件是否存在，存在则返回 
	                  if(locaFile.exists()){ 
	                      return; 
	                  }else{ 
	                      outputStream = new FileOutputStream(relativeLocalPath+ ftpFile.getName()); 
	                      ftp.retrieveFile(ftpFile.getName(), outputStream); 
	                      outputStream.flush(); 
	                      outputStream.close(); 
	                  } 
	              } catch (Exception e) { 
	                  logger.error(e);
	              } finally { 
	                  try { 
	                      if (outputStream != null){ 
	                          outputStream.close(); 
	                      }
	                  } catch (IOException e) { 
	                     logger.error("输出文件流异常"); 
	                  } 
	              } 
	          } 
	      } else { 
	          String newlocalRelatePath = relativeLocalPath + ftpFile.getName(); 
	          String newRemote = new String(relativeRemotePath+ ftpFile.getName().toString()); 
	          File fl = new File(newlocalRelatePath); 
	          if (!fl.exists()) { 
	              fl.mkdirs(); 
	          } 
	          try { 
	              newlocalRelatePath = newlocalRelatePath + '/'; 
	              newRemote = newRemote + "/"; 
	              String currentWorkDir = ftpFile.getName().toString(); 
	              boolean changedir = ftp.changeWorkingDirectory(currentWorkDir); 
	              if (changedir) { 
	                  FTPFile[] files = null; 
	                  files = ftp.listFiles(); 
	                  for (int i = 0; i < files.length; i++) { 
	                      downloadFile(files[i], newlocalRelatePath, newRemote); 
	                  } 
	              } 
	              if (changedir){
	                  ftp.changeToParentDirectory(); 
	              } 
	          } catch (Exception e) { 
	              logger.error(e);
	          } 
	      } 
	  } 

	  
	  public static void main(String[] args) throws Exception{  
		 /**
		  *  * 文件名  
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
 * 20171103_时间戳.dat
		  */
		  
		  
		  
		  //亦庄203
			Ftp f = new Ftp();
			f.setIpAddr("10.139.0.130");
			f.setPath("/app/jzyx/model_outomatic_put_files/parameter_js_files");
			f.setPort(21);
			f.setPwd("AhvRab5!");
			f.setUserName("odsftp");
			FtpUtil.connectFtp(f);
//	        File file = new File("/home/ctgmkt/jzyx/yhl/bbb.txt");  
			
			File file = new File(args[0]);  
			logger.info("参数++++++++++++++++++++++++++++++++++++++++++++++"+args[0]);
	        FtpUtil.upload(file);//把文件上传在ftp上
	        logger.info("上传203完毕++++++++++++++++++++++++++++++++++++++++++++++++++++");
		  
		  
		  
		  
		  //昌平205
//			Ftp f = new Ftp();
//			f.setIpAddr("10.140.16.205");
//			f.setPath("/home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files");
//			f.setPort(21);
//			f.setPwd("ctgmkt@123");
//			f.setUserName("ctgmkt");
//			FtpUtil.connectFtp(f);
////	        File file = new File("/home/ctgmkt/jzyx/yhl/bbb.txt");  
//			File file = new File(args[0]);  
//	        FtpUtil.upload(file);//把文件上传在ftp上
	        
	        
	     }  
	  
}
