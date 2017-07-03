package hbase_service;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Table;


//java -cp 0616_testHbaseService2.jar:./hbase/*:./json/* hbase_service.UserFeatures

public class HbaseUtils2 {

	
	/**
	 * 这版不用，有问题
	 */
	
	
	
	
	
	
	
	
	
	
	
	
	 // 声明静态配置
    static Configuration conf = null;
    static HConnection conn = null;
    static {
    	
        conf = HBaseConfiguration.create();
        
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum","BQJ-EDA-XH628-2015029,BQJ-EDA-XH628-2015047,BQJ-EDA-XH628-2015027");
        try {
			conn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
  //基本信息表
	public  static Table mkt_portrait_info() throws IOException{
		Table table = conn.getTable("mkt_portrait_info");  
//		HTableInterface table = conn.getTable("mkt_portrait_info");
		return table;
	}

	
	
	public static void closeTable(Table table) throws IOException{

		conn.close();
	}
	
	
	
	
	

	//量收表
	public  static Table mkt_portrait_trail() throws IOException{
//		Connection connection = ConnectionFactory.createConnection(conf);
//		Table table = connection.getTable(TableName.valueOf("mkt_portrait_trail"));  
		Table table = conn.getTable("mkt_portrait_trail");
		return table;

	}
	//图片
	public  static Table mkt_portrait_feature_dimension() throws IOException{
//		Connection connection = ConnectionFactory.createConnection(conf);
//		Table table = connection.getTable(TableName.valueOf("mkt_portrait_feature_dimension"));  
		Table table = conn.getTable("mkt_portrait_feature_dimension");
		return table;

	}
	}


	
	
	
	
	


