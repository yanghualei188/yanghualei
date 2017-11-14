package test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class OSExecuter {
	  
    /** 
     * 既可以用于linux也可以用于window
     * <b>command。</b>   
     * <p><b>详细说明：</b></p> 
     * <!-- 在此添加详细说明 --> 
     * 无。 
     * @param command 
     */  
    public static void command(String command){  
        try  
        {  
            Process process=new ProcessBuilder(Arrays.asList(command.split(" "))).start();  //  \\s+
            //标准输入流  
            BufferedReader result= new BufferedReader(new InputStreamReader(process.getInputStream()));  
            String s=result.readLine();  
            while(s!=null){  
                System.out.println(s);  
                s=result.readLine();  
            }  
            //标准错误输入流  
            BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));  
            s=error.readLine();  
            while(s!=null){  
                System.err.println(s);  
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
    public static void main(String[] args) {  
//      OSExecuter.command("dir");  
//    	OSExecuter.command("jps");  


    	//昌平
//    	OSExecuter.command("java -cp /home/weblogic/test/1106_ftp.jar ftp.FtpUtil /home/weblogic/test/20171107_12.dat"); 从155ftp到205

    	
    	
    	
    	//亦庄
//    	OSExecuter.command("java -cp /home/weblogic/test/1106_ftp.jar ftp.FtpUtil "+args[0]);从155ftp到205
//    	OSExecuter.command("java -cp /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/1106_test.jar ftp.FtpUtil /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files/" + args[0]);
//    	OSExecuter.command("java -cp /home/weblogic/app/ailkapp/alik-precisionMarket/local/1106_test.jar ftp.FtpUtil /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files/" + args[0]);

    	
//    	OSExecuter.command("expect /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/yz.exp " + args[0]);
//    	OSExecuter.command("expect /home/weblogic/test/yz.exp " + args[0]);
    	
    	OSExecuter.command("java -cp /home/weblogic/test/1106_bbb.jar test.OSExecuter 20171107_1510051884690.dat");
    }  
}
