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
 * �ļ���  
 * 20171103_ʱ���.dat        ---->  20171103_1.dat.bak
 * 20171103_ʱ���.dat
 * 20171103_ʱ���.dat
 * 20171103_ʱ���.dat
 * 20171103_ʱ���.dat
 * @author Administrator
 *
 */
public class FileDispatchMain {
	public static void main(String[] args) throws ClassNotFoundException, IOException {

		//ÿ�����л�ȡ����ʱ�������С���Ǹ��ļ�
		String targetFileName = Utils.getTargetFileName();
		System.out.println(targetFileName);
		
		if(targetFileName == null){
			
		}else{

			//��ȡ�ļ������ݣ�������list
			List<String> params = Utils.parsedFile(targetFileName);
		    //д���ļ�

			Utils.writeToTxt(params,targetFileName);
			
			//��shell ��shell�е��߼�������1��ԭʼ���ݽ��д��������·����ݣ�2 �������·������� ���ղ��õ�ʡ�ݷֱ���96  97 ������ִ���·��߼�
			//��ƽ
//			Utils.command("sh /home/ctgmkt/mktag_shell_file/model_outomatic_put_files/execute_main_shell.sh "  + targetFileName);
			//��ׯ
//			Utils.command("sh /app/jzyx/model_outomatic_put_files/execute_main_shell.sh "  + targetFileName);
			//�����Ѿ���������ļ���
//			Utils.changeTargetFileName(targetFileName);
			//��ƽҪ�������
//			System.out.println("aaabbbb");
		}
		
		
		
	}
}
