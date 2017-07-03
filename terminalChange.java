package hbase_basic_api;







import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class terminalChange extends Mapper<LongWritable,
Text, ImmutableBytesWritable, Put>{

protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   String line = value.toString();
	   String[] items = line.split("\t");  //少杰宽表


	   
	   if(items[0].length() == 0){
		   
	   }else{

		 ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(items[0]));
		 Put put = new Put(Bytes.toBytes(items[0]));  

	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("pref_chg_trmnl_prob"), Bytes.toBytes(items[1]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_up"), Bytes.toBytes(items[2]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("pref_replace_freq"), Bytes.toBytes(items[3]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("avg_price_2year"), Bytes.toBytes(items[4]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("pref_screen_size"), Bytes.toBytes(items[5]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("pref_opera_system"), Bytes.toBytes(items[6]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("pref_new"), Bytes.toBytes(items[7]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("pref_festval"), Bytes.toBytes(items[8]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_trmnl_low"), Bytes.toBytes(items[9]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_trmnl_hight"), Bytes.toBytes(items[10]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_trmnl_face"), Bytes.toBytes(items[11]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_trmnl_thrifty"), Bytes.toBytes(items[12]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_samsang_fans"), Bytes.toBytes(items[13]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_apple_fans"), Bytes.toBytes(items[14]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_huawei_fans"), Bytes.toBytes(items[15]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_xiaomi_fans"), Bytes.toBytes(items[16]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_oppo_fans"), Bytes.toBytes(items[17]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_vivo_fans"), Bytes.toBytes(items[18]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("is_chinese_fans"), Bytes.toBytes(items[19]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_best_trmnl_brand"), Bytes.toBytes(items[20]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_best_trmnl_mdl"), Bytes.toBytes(items[21]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_best_trmnl_prod_id"), Bytes.toBytes(items[22]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_price_trmnl_brand"), Bytes.toBytes(items[23]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_price_trmnl_mdl"), Bytes.toBytes(items[24]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_price_trmnl_prod_id"), Bytes.toBytes(items[25]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_up_trmnl_brand"), Bytes.toBytes(items[26]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_up_trmnl_mdl"), Bytes.toBytes(items[27]));
	     put.add(Bytes.toBytes("cf"), Bytes.toBytes("reco_up_trmnl_prod_id"), Bytes.toBytes(items[28]));
     

     		context.write(rowkey, put);
	   }
    }


public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    final String INPUT_PATH= args[0];
    final String OUTPUT_PATH= args[1];
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum","BQJ-EDA-XH628-2015029,BQJ-EDA-XH628-2015047,BQJ-EDA-XH628-2015027");

//    conf.setLong("hbase.client.scanner.max.result.size",2147483648L);
    

    
//   CodedInputStream.setSizeLimit(44);
    
//    conf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03");
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf(args[2]));
    
//    table.setAutoFlushTo(false);
    //设置缓冲大小
//    table.setWriteBufferSize(20*1024*1024);

    Job job=Job.getInstance(conf);
//    job.getConfiguration().set("mapred.map.tasks", "80");
    
    job.getConfiguration().set("mapreduce.reduce.shuffle.memory.limit.percent", "0.06");//
//    job.getConfiguration().set("mapred.map.child.java.opts", args[3]); //"-Xmx4096m"

    job.setJarByClass(terminalChange.class);
    job.setMapperClass(terminalChange.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setOutputFormatClass(HFileOutputFormat2.class);
    HFileOutputFormat2.configureIncrementalLoad(job,table,connection.getRegionLocator(TableName.valueOf(args[2])));
    
//    table.flushCommits();
    
    FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
    FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
    System.exit(job.waitForCompletion(true)?0:1);
}
}

