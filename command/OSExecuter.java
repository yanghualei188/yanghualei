package test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class OSExecuter {
	  
    /** 
     * �ȿ�������linuxҲ��������window
     * <b>command��</b>   
     * <p><b>��ϸ˵����</b></p> 
     * <!-- �ڴ������ϸ˵�� --> 
     * �ޡ� 
     * @param command 
     */  
    public static void command(String command){  
        try  
        {  
            Process process=new ProcessBuilder(Arrays.asList(command.split(" "))).start();  //  \\s+
            //��׼������  
            BufferedReader result= new BufferedReader(new InputStreamReader(process.getInputStream()));  
            String s=result.readLine();  
            while(s!=null){  
                System.out.println(s);  
                s=result.readLine();  
            }  
            //��׼����������  
            BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));  
            s=error.readLine();  
            while(s!=null){  
                System.err.println(s);  
                s=error.readLine();  
            }  
        } catch (Exception e)  
        {  
            //����  
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


    	//��ƽ
//    	OSExecuter.command("java -cp /home/weblogic/test/1106_ftp.jar ftp.FtpUtil /home/weblogic/test/20171107_12.dat"); ��155ftp��205

    	
    	
    	
    	//��ׯ
//    	OSExecuter.command("java -cp /home/weblogic/test/1106_ftp.jar ftp.FtpUtil "+args[0]);��155ftp��205
//    	OSExecuter.command("java -cp /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/1106_test.jar ftp.FtpUtil /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files/" + args[0]);
//    	OSExecuter.command("java -cp /home/weblogic/app/ailkapp/alik-precisionMarket/local/1106_test.jar ftp.FtpUtil /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/paratemer_js_files/" + args[0]);

    	
//    	OSExecuter.command("expect /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/yz.exp " + args[0]);
//    	OSExecuter.command("expect /home/weblogic/test/yz.exp " + args[0]);
    	
    	OSExecuter.command("java -cp /home/weblogic/test/1106_bbb.jar test.OSExecuter 20171107_1510051884690.dat");
    }  
}
