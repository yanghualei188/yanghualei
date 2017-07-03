package hbase_service;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Table;

//java -cp 0616_testHbaseService2.jar:./hbase/*:./json/* hbase_service.UserFeatures

	public class HbaseUtils3 {
		 // 声明静态配置
	    static Configuration conf = null;
	    static HConnection pool = null;
	    static {
	        conf = HBaseConfiguration.create();
	        conf.set("hbase.zookeeper.property.clientPort", "2181");
	        conf.set("hbase.zookeeper.quorum","BQJ-EDA-XH628-2015029,BQJ-EDA-XH628-2015047,BQJ-EDA-XH628-2015027");
	        ExecutorService threads  = Executors.newFixedThreadPool(10); 
	        try {
				pool = HConnectionManager.createConnection(conf, threads);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	    }
	  //基本信息表
		public  static Table mkt_portrait_info() throws IOException{
			

			
			HTableInterface hTable = pool.getTable("mkt_portrait_info");
			return hTable;
		}
		//量收表
		public  static Table mkt_portrait_trail() throws IOException{

			HTableInterface hTable = pool.getTable("mkt_portrait_trail");
			return hTable;

		}
		//图片
		public  static Table mkt_portrait_feature_dimension() throws IOException{

			HTableInterface hTable = pool.getTable("mkt_portrait_feature_dimension");
			return hTable;
		}
		
		public  static void  closePool(Table table) throws IOException{

			pool.close();

		}
	}


	
	
	
	
	


