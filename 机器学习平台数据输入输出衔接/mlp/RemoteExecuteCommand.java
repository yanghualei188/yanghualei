package mlp;
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


/**
 * java -cp commons-io-2.4.jar:commons-lang-2.6.jar:ganymed-ssh2-build210.jar:1108_remote_OS.jar test.RemoteExecuteCommand
 *
 *commons-io-2.4.jar
commons-lang-2.6.jar
ganymed-ssh2-build210.jar
 *
 */
/** 
 * 远程执行linux的shell script 
 * @author Ickes 
 * @since  V0.1 
 */  
public class RemoteExecuteCommand {  
    //字符编码默认是utf-8  
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

    public Boolean login(){  
        boolean flg=false;  
        try {  
            conn = new Connection(ip);  
            conn.connect();//连接  
            flg=conn.authenticateWithPassword(userName, userPwd);//认证  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return flg;  
    }  

    public String execute(String cmd){  
        String result="";  
        try {  
            if(login()){  
                Session session= conn.openSession();//打开一个会话  
                session.execCommand(cmd);//执行命令  
                result=processStdout(session.getStdout(),DEFAULTCHART);  
                //如果为得到标准输出为空，说明脚本执行出错了  
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
      

    public String executeSuccess(String cmd){  
        String result="";  
        try {  
            if(login()){  
                Session session= conn.openSession();//打开一个会话  
                session.execCommand(cmd);//执行命令  
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
    * 解析脚本执行返回的结果集 
    * @author Ickes 
    * @param in 输入流对象 
    * @param charset 编码 
    * @since V0.1 
    * @return 
    *       以纯文本的格式返回 
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

    	
    	
	}
    
    
    
}  
