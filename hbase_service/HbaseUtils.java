package hbase_service;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

//java -cp 0616_testHbaseService2.jar:./hbase/*:./json/* hbase_service.UserFeatures

	public class HbaseUtils {
		 // ������̬����
	    static Configuration conf = null;
	    static {
	        conf = HBaseConfiguration.create();
	        conf.set("hbase.zookeeper.property.clientPort", "2181");
	        conf.set("hbase.zookeeper.quorum","BQJ-EDA-XH628-2015029,BQJ-EDA-XH628-2015047,BQJ-EDA-XH628-2015027");
	    }
	  //������Ϣ��
		public  static Table mkt_portrait_info() throws IOException{
	//		ExecutorService executor = Executors.newFixedThreadPool(100);
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf("mkt_portrait_info"));  
			return table;
		}
		//���ձ�
		public  static Table mkt_portrait_trail() throws IOException{
	//		ExecutorService executor = Executors.newFixedThreadPool(1000);
	//		Connection connection = ConnectionFactory.createConnection(conf,executor);
			
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf("mkt_portrait_trail"));  
			return table;
		}
		//ͼƬ
		public  static Table mkt_portrait_feature_dimension() throws IOException{
	//		ExecutorService executor = Executors.newFixedThreadPool(1000);
	//		Connection connection = ConnectionFactory.createConnection(conf,executor);
			
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf("mkt_portrait_feature_dimension"));  
			return table;
		}
	}


	
	
	
	
	


