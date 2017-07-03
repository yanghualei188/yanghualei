package hbase_b_V3;
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
import org.apache.hadoop.hbase.client.Table;


/**
 * 生产环境实际是用的这个版本的hbase连接
 * @author Administrator
 *
 */


public class HbaseUtils2 {
	 // 声明静态配置
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.zookeeper.quorum","BQJ-EDA-XH628-2015029,BQJ-EDA-XH628-2015047,BQJ-EDA-XH628-2015027");
        conf.set("hbase.zookeeper.quorum","10.140.16.203,10.140.16.201,10.140.16.221");
        conf.set("zookeeper.session.timeout", "400000");   //最新加的这个参数
    }
    
	public  static Table getHTable(String threeTop) throws IOException{

		
		
		ExecutorService executor = Executors.newFixedThreadPool(3000);
		Connection connection = ConnectionFactory.createConnection(conf,executor);
		

		Table table = null;
		if(threeTop.equals("100")){
			table = connection.getTable(TableName.valueOf("ta_mktag_total_day_hbase"));  
		
		}
		
		if(threeTop.equals("101")){
			table = connection.getTable(TableName.valueOf("ta_mktag_yiwang"));  
		 
		}
		

		if(threeTop.equals("102")){
			table = connection.getTable(TableName.valueOf("ta_xqcs"));  
	
		}


		if(threeTop.equals("103")){
			table = connection.getTable(TableName.valueOf("ta_jiaoyan"));  
	
		}
		
		if(threeTop.equals("104")){
			table = connection.getTable(TableName.valueOf("ta_khdty"));  
	
		}
		
		return table;

		}
	

	
	public  static Table getSlaveHTable() throws IOException{

		
		
//		ExecutorService executor = Executors.newFixedThreadPool(1000);
//		Connection connection = ConnectionFactory.createConnection(conf,executor);
		
//		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = null;
//		table = connection.getTable(TableName.valueOf("ta_slave_queryinfo")); 

		return table;

		}
	
	
	
	
	
	


	}


	
	
	
	
	


