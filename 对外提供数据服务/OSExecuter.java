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
            Process process=new ProcessBuilder(Arrays.asList(command.split(" "))).start();  
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

    	System.out.println("dddddddddddddddddddddddddddd");
    	int a = 0;
    	for(int i = 0 ; i < 5 ; i++){
    		OSExecuter.command("curl "+"http://10.140.16.174:8090/qryUserProIsRight?user=bdms0001&pwd=bdms000120170412&channelId=4&actionID=201601010001&modelID=8109990001&qryUserProRuleId=100111100&targetTypeId=1&prvcID=821&targetObjectId=13308900000");
    		a++;
    	}
    		System.out.println(a);
    		
    }  
}
