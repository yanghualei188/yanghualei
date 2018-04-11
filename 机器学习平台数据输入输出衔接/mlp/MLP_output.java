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
		/**�����ļ���ʱ��ȥ����
		 * ��ȥ���Ѿ���������ļ�
		 * ����δ��������ļ�ʱ����scp �� 205 ��Զ�̵�½��205 �� load ��ȥִ��hive���� ��ʽ��  
		 * ��Զ�̵�½205�ϵ�79ftp ���ļ��ϴ���79ftp��
		 * 
		 * 
		 * �ص��߼�
		 * ��ʱ��    ȥget�ļ�  ��load�� ִ��hive�ű� �� ���뵽ָ����hive��ȫ�����ǣ�
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
				//scp �� 205 
				OSExecuter.command("expect /home/mlp_user/yhl/scp.exp 10.140.16.205 ctgmkt ctgmkt@123 /home/mlp_user/mlp_out/"+lastFileName+" /tmp/mlp_out/"+lastFileName);
				//��Զ�̵�½205�ϵ�79ftp ���ļ��ϴ���79ftp��
			    RemoteExecuteCommand rec=new RemoteExecuteCommand("10.140.16.205", "ctgmkt","ctgmkt@123"); 
			    //ִ�б��ؽű������߼���7+50
			    rec.executeSuccess("gawk -f /home/ctgmkt/yhl/etl.gawk /tmp/mlp_out/"+lastFileName+" 1>/tmp/mlp_out/"+lastFileName + "_ed");
			    rec.executeSuccess("sed -i -f /home/ctgmkt/yhl/etl.sed /tmp/mlp_out/" + lastFileName + "_ed");
			    //ִ��205��ftp��ftp������
			    rec.executeSuccess("expect /home/ctgmkt/yhl/ftp_put.exp " + lastFileName + "_ed");
			}
            br.close();
			//��Ҫ��readed_files��д���µ��ļ� ׷��д��
		    FileWriter fw = new FileWriter("/home/mlp_user/yhl/readed_flies",true);
		    fw.write(lastFileName);
		    fw.flush();
		    fw.close();
 
		
		}

		
		
	}
}
