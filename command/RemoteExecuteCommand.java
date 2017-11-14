package test;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import sun.java2d.pipe.hw.AccelGraphicsConfig;

//import org.apache.commons.lang.StringUtils;
//
//import ch.ethz.ssh2.Connection;
//import ch.ethz.ssh2.Session;
//import ch.ethz.ssh2.StreamGobbler;  
  

//http://blog.csdn.net/u013089991/article/details/52448989

/**
 * java -cp commons-io-2.4.jar:commons-lang-2.6.jar:ganymed-ssh2-build210.jar:1108_remote_OS.jar test.RemoteExecuteCommand
 *
 *commons-io-2.4.jar
commons-lang-2.6.jar
ganymed-ssh2-build210.jar
 *
 */
/** 
 * Զ��ִ��linux��shell script 
 * @author Ickes 
 * @since  V0.1 
 */  
public class RemoteExecuteCommand {  
    //�ַ�����Ĭ����utf-8  
    private static String  DEFAULTCHART="UTF-8";  
    private Connection conn;  
    private String ip;  
    private String userName;  
    private String userPwd;  
      
    public RemoteExecuteCommand(String ip, String userName, String userPwd) {  
        this.ip = ip;  
        this.userName = userName;  
        this.userPwd = userPwd;  
    }  
      
      
    public RemoteExecuteCommand() {  
          
    }  
      
    /** 
     * Զ�̵�¼linux������ 
     * @author Ickes 
     * @since  V0.1 
     * @return 
     *      ��¼�ɹ�����true�����򷵻�false 
     */  
    public Boolean login(){  
        boolean flg=false;  
        try {  
            conn = new Connection(ip);  
            conn.connect();//����  
            flg=conn.authenticateWithPassword(userName, userPwd);//��֤  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return flg;  
    }  
    /** 
     * @author Ickes 
     * Զ��ִ��shll�ű��������� 
     * @param cmd 
     *      ����ִ�е����� 
     * @return 
     *      ����ִ����󷵻صĽ��ֵ 
     * @since V0.1 
     */  
    public String execute(String cmd){  
        String result="";  
        try {  
            if(login()){  
                Session session= conn.openSession();//��һ���Ự  
                session.execCommand(cmd);//ִ������  
                result=processStdout(session.getStdout(),DEFAULTCHART);  
                //���Ϊ�õ���׼���Ϊ�գ�˵���ű�ִ�г�����  
                if(StringUtils.isBlank(result)){  
                    result=processStdout(session.getStderr(),DEFAULTCHART);  
                }  
                conn.close();  
                session.close();  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return result;  
    }  
      
      
    /** 
     * @author Ickes 
     * Զ��ִ��shll�ű��������� 
     * @param cmd 
     *      ����ִ�е����� 
     * @return 
     *      ����ִ�гɹ��󷵻صĽ��ֵ���������ִ��ʧ�ܣ����ؿ��ַ���������null 
     * @since V0.1 
     */  
    public String executeSuccess(String cmd){  
        String result="";  
        try {  
            if(login()){  
                Session session= conn.openSession();//��һ���Ự  
                session.execCommand(cmd);//ִ������  
                result=processStdout(session.getStdout(),DEFAULTCHART);  
                conn.close();  
                session.close();  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return result;  
    }  
      
   /** 
    * �����ű�ִ�з��صĽ���� 
    * @author Ickes 
    * @param in ���������� 
    * @param charset ���� 
    * @since V0.1 
    * @return 
    *       �Դ��ı��ĸ�ʽ���� 
    */  
    private String processStdout(InputStream in, String charset){  
    	InputStream    stdout = new StreamGobbler(in);
        StringBuffer buffer = new StringBuffer();;  
        try {  
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout,charset));  
            String line=null;  
            while((line=br.readLine()) != null){  
                buffer.append(line+"\n");  
            }  
        } catch (UnsupportedEncodingException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return buffer.toString();  
    }  
      
    public static void setCharset(String charset) {  
        DEFAULTCHART = charset;  
    }  
    public Connection getConn() {  
        return conn;  
    }  
    public void setConn(Connection conn) {  
        this.conn = conn;  
    }  
    public String getIp() {  
        return ip;  
    }  
    public void setIp(String ip) {  
        this.ip = ip;  
    }  
    public String getUserName() {  
        return userName;  
    }  
    public void setUserName(String userName) {  
        this.userName = userName;  
    }  
    public String getUserPwd() {  
        return userPwd;  
    }  
    public void setUserPwd(String userPwd) {  
        this.userPwd = userPwd;  
    }  
    public static void main(String[] args) {
//    	RemoteExecuteCommand rec=new RemoteExecuteCommand("192.168.238.133", "root","ickes");  
//        //ִ������  
//        System.out.println(rec.execute("ifconfig"));  
//        //ִ�нű�  
//        rec.execute("sh /usr/local/tomcat/bin/statup.sh");  
//        //�����������������������ǣ�����ķ���������ִ�гɹ���񶼷��أ�  
//        //��������أ����������߽ű�ִ�д��󽫷��ؿ��ַ���  
//        rec.executeSuccess("ifconfig");  
    	
    	
    	  RemoteExecuteCommand rec=new RemoteExecuteCommand("10.140.16.205", "ctgmkt","ctgmkt@123");  
	      //ִ������  
//	      System.out.println(rec.execute("ifconfig"));  
	      //ִ�нű�  
//	      String aa = args[0];
//	      rec.execute("java -cp /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/1106_test.jar ftp.FtpUtil /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files/"+aa);  
	      //�����������������������ǣ�����ķ���������ִ�гɹ���񶼷��أ�  
	      //��������أ����������߽ű�ִ�д��󽫷��ؿ��ַ���  
//	      rec.executeSuccess("java -cp /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/1106_test.jar ftp.FtpUtil /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files/"+aa);  
	      rec.executeSuccess("java -cp /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/1106_FileDispatchMain.jar ftp.FileDispatchMain");  
    	
    	
    	
	}
    
    
    
}  
