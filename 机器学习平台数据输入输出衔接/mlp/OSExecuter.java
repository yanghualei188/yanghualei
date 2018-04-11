package mlp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class OSExecuter {

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

    	
    	
    }  
}
