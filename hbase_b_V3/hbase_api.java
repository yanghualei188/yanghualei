package hbase_b_V3;
import java.net.URLDecoder;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


import net.sf.json.JSONObject;

import redis.clients.jedis.Jedis;


public class hbase_api {
	/**
	 * 
	 * user 用户名
	 * pwd  密码
	 * channelId 渠道ID
	 * actionID  活动ID
	 * modelID   模型ID
	 * qryUserProRuleId   规则ID
	 * targetTypeId  查询用户类型ID   要是一个jsonArray对象 
	 * prvcID    标准省分编码
	 * targetObjectId    查询用户对象ID 
	 * 
	 *   
	 *   
	 *   => ["t1", "table1"]
		hbase(main):004:0> put 't1','user1','cf:pwd1','123'
		0 row(s) in 0.4300 seconds
		
		hbase(main):005:0> put 't1','iphone','cf:name','name'
		0 row(s) in 0.0410 seconds
		
		hbase(main):006:0> put 't1','iphone','cf:age','age'
		0 row(s) in 0.0370 seconds
		
		hbase(main):007:0> put 't1','iphone','cf:height','height'
		0 row(s) in 0.0250 seconds
		
		hbase(main):008:0> put 't1','iphone','cf:weight','weight'
		0 row(s) in 0.0320 seconds
	 *   
	 *   命令  java -cp .:0320_test_hbase13.jar:/usr/local/hbase-0.99.2/lib/*:./json/*:./slf4j/* hbase_api 
	 *   
	 *    java -cp .:0321_redispool7.jar:./redis/* redis.Test_jedisPool 
	 */
//	private static final Logger logger = LoggerFactory.getLogger(hbase_api.class);  
	public static String resolve_json(String jsonstr) throws Exception{
		
		String user = null;
		String pwd = null;
		String qryUserProRuleId = null;
		String targetObjectId = null;
		String prvcID = null;
		String targetObjectInfo = null;
		
		
		JSONObject object_source = new JSONObject();
		String[] items = jsonstr.split("&");

		for(int i = 0 ; i < items.length ; i++){
			 if(items[i].contains("=")){
				 String[] is = items[i].split("=");
				 object_source.put(is[0],is[1]);  
			 }
		}
	
		if(items.length == 9){
			user = object_source.getString("user");
			pwd = object_source.getString("pwd");	
			qryUserProRuleId = object_source.getString("qryUserProRuleId");		
			targetObjectId = object_source.getString("targetObjectId");	
			prvcID = object_source.getString("prvcID");		
		}
		if(items.length == 10){
			user = object_source.getString("user");
			pwd = object_source.getString("pwd");	
			qryUserProRuleId = object_source.getString("qryUserProRuleId");
			targetObjectId = object_source.getString("targetObjectId");
			prvcID = object_source.getString("prvcID");
			
			targetObjectInfo = URLDecoder.decode(object_source.getString("targetObjectInfo"), "UTF-8");
//			targetObjectInfo = object_source.getString("targetObjectInfo");
		}


		
		String targetObjectId1 =  null;
		String threeTop = qryUserProRuleId.substring(0,3);
	
		//截取规则id的头三位  当是101  时  是大宽表  rulekey就是  规则id本身， 当是  102时，是异网表，  rulekey是  规则id加身份id
		if(threeTop.equals("100")){
			targetObjectId1 = reverse1(targetObjectId);
			
		}
		if(threeTop.equals("101")){
//			targetObjectId1 = reverse1(targetObjectId.split("_")[0]);
			targetObjectId1 = reverse1(targetObjectId);
		}
		
		if(threeTop.equals("102")){
			targetObjectId1 = reverse1(targetObjectId);
			
		}
		
		if(threeTop.equals("103")){
			targetObjectId1 = reverse1(targetObjectId) +"_"+ prvcID;
			
		}
		

		if(threeTop.equals("104")){
			targetObjectId1 = targetObjectId;
			
		}
		
		JSONObject object = new JSONObject();
		Table table = null;
		Jedis jedis = redisUtils.pool.getResource(); 
		try{
			
			table = HbaseUtils.getHTable(threeTop);
			
		     //先判断用户名是否存在   如果不存在
		     if (!jedis.exists(user)) {
		    	  object.put("ResultCode","1"); 
			 }
		     if (jedis.exists(user) && !jedis.get(user).equals(pwd)) {
		    	 object.put("ResultCode","2");
			 }
			 if(jedis.exists(user) && jedis.get(user).equals(pwd)){
				 if(!table.exists(new Get(Bytes.toBytes(targetObjectId1))) 
						&& threeTop.equals("100") 
						&& threeTop.equals("102")
						&& threeTop.equals("103")
						&& threeTop.equals("104")
						 ){       
					 object.put("ResultCode","-1");
					 return object.toString();
				 }
				 if(!table.exists(new Get(Bytes.toBytes(targetObjectId1+"_1"))) && threeTop.equals("101")){       //这里为了照顾scan
					 object.put("ResultCode","-1");
					 return object.toString();
				 }
				 
				 Get get = new Get(Bytes.toBytes(targetObjectId1));
				 
				 Put put = new Put(Bytes.toBytes(""+System.currentTimeMillis()));   //这里得改
				 put.setWriteToWAL(false);
			

				 
				 //这里加上对校验的逻辑   从  某个字段中
				 if(targetObjectInfo != null && !targetObjectInfo.contains(",")){
					 
					 String[] testInfos = targetObjectInfo.split("-");
					 get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(testInfos[0]));
					 Result result = table.get(get);
					 
					 String value = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(testInfos[0])));
					 if(!value.equals(testInfos[1])){
						 //这里不满足两个动作，一是往hbase插入查询的条件，二是给用户返回错误代码
						 object.put("targetObjectInfoIsright","0"); 
						 object.put("targetObjectId",targetObjectId); 
						 
//						 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"jy"));
//						 table1.put(put);
//						 table1.flushCommits();
						 
						 return object.toString();						 
					 }else{
						 object.put("targetObjectInfoIsright","1"); 		
						 object.put("targetObjectId",targetObjectId); 
						 
//						 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"jy"));
//						 table1.put(put);
//						 table1.flushCommits();
						 
						 return object.toString();
					 }
					 
				 }else if(targetObjectInfo != null &&  targetObjectInfo.contains(",")){
					 for(String e: targetObjectInfo.split(",")){
						 
						 
						 String[] testInfos = targetObjectInfo.split("-");
						 get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(testInfos[0]));
						 Result result = table.get(get);
						 
						 String value = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(testInfos[0])));
						 if(!value.equals(testInfos[1])){
							 //这里不满足两个动作，一是往hbase插入查询的条件，二是给用户返回错误代码
							 object.put("targetObjectInfoIsright","0"); 
							 object.put("targetObjectId",targetObjectId); 
							 
//							 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"jy"));
//							 table1.put(put);
//							 table1.flushCommits();
							 
							 return object.toString();
						 }else{
							 object.put("targetObjectInfoIsright","1"); 
							 object.put("targetObjectId",targetObjectId); 
							 
//							 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"jy"));
//							 table1.put(put);
//							 table1.flushCommits();
							 
							 return object.toString();
						 }
					 }
				 }

				 if (jedis.hget(qryUserProRuleId,"sq") != null) {
					 //加上解析判断条件的方法
					 //  a>0andb<7orc>6andd<10
					 Set<String> sets = resolve_rules.resolve_to_sets(jedis.hget(qryUserProRuleId,"sq"));
					 //满足简单条件    ！！！！！！  不满足时是给退出的
					 if(!resolve_rules.resolve_main(sets, table,get)){
						 //这里不满足两个动作，一是往hbase插入查询的条件，二是给用户返回错误代码
						 object.put("ResultCode","3"); 
//						 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"sq"));
//						 table1.put(put);
//						 table1.flushCommits();
						 return object.toString();
					 }
				 }
				 
				 if (jedis.hget(qryUserProRuleId,"cq") != null) {
					 String cqString = jedis.hget(qryUserProRuleId,"cq");
					 if(cqString.contains("and")){
						 
						 
						 String[] cqArray = cqString.split("and");
						 for(int i = 0 ; i < cqArray.length ; i++){
							 String[] kv = cqArray[i].split("=");
							 
							 get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(kv[0].replace("h_", "")));
							 Result result = table.get(get);
							 
							 String value = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(kv[0].replace("h_", ""))));
							 if(!value.equals(kv[1])){
								 //这里不满足两个动作，一是往hbase插入查询的条件，二是给用户返回错误代码
								 object.put("ResultCode","3"); 
								 
//								 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"cq"));
//								 table1.put(put);
//								 table1.flushCommits();
								 return object.toString();
							 }
						 }
					 }else{
						 
						 
						 String[] kv = cqString.split("=");
						 
						 get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(kv[0].replace("h_", "")));
						 Result result = table.get(get);
						 
						 String value = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(kv[0].replace("h_", ""))));
						 if(!value.equals(kv[1])){
							 //这里不满足两个动作，一是往hbase插入查询的条件，二是给用户返回错误代码
							 object.put("ResultCode","3"); 
							 
//							 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr+"_"+"cq"));
//							 table1.put(put);
//							 table1.flushCommits();
							 return object.toString();
						 }
						 
						 
						 
					 }
				
				 }

				 //返回要获得的字段
				 if(jedis.hget(qryUserProRuleId,"fed") != null){
					 //从redis中的表中找到过滤条件字段
			
					 String select = jedis.hget(qryUserProRuleId,"fed");
					 String[] selects = select.split(",");
					 for(int i = 0 ; i < selects.length ;i++){
						 
						
						 
						 if(selects[i].startsWith("m")){
							 String m_result = jedis.get(qryUserProRuleId+"_"+prvcID);  
//							 String m_result_bm = new String(m_result.getBytes("gbk"),"utf-8");   //重新编码  看当初redis的中文以什么编码进去的
							 
							 object.put("add_info",m_result);
						 }
						 if(selects[i].startsWith("ta_mktag_yiwang_")){
//							 get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(selects[i].replace("ta_mktag_yiwang_", "")));
//							 Result result = table.get(get);
//							 String h_result = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(selects[i].replace("ta_mktag_yiwang_", ""))));
//							 object.put(selects[i].replace("ta_mktag_yiwang_", ""),h_result); 
							 
							   Scan s = new Scan();  
							   s.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(selects[i].replace("ta_mktag_yiwang_", "")));  //这是要找的列
							   s.setStartRow(Bytes.toBytes(targetObjectId1+"_1"));   //这是要开始找的行
						       s.setFilter(new PrefixFilter(targetObjectId1.getBytes()));  
						       ResultScanner rs = table.getScanner(s);  
						       int num = 1;
						       for (Result r : rs) {  
						    	   
						    	   String h_result = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes(selects[i].replace("ta_mktag_yiwang_", ""))));
						    	   object.put(selects[i].replace("ta_mktag_yiwang_", "")+""+num,h_result);
						    	   num ++;
						       }
							 
						 }
						 if(selects[i].startsWith("h_")){
							 get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(selects[i].replace("h_", "")));
							 Result result = table.get(get);
							 String h_result = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(selects[i].replace("h_", ""))));
							 object.put(selects[i].replace("h_", ""),h_result); 
						 }
					 }
					 
					 
					 //加上手机号
					 
					 object.put("targetObjectId",targetObjectId); 
					 
					 
//					 put.add(Bytes.toBytes("cf"), Bytes.toBytes("queryinfo"), Bytes.toBytes(jsonstr));
//					 table1.put(put);
//					 table1.flushCommits();
				 } 
			 }
			 
		

		
//		jedis.close();
		}catch(Exception e){
			throw e;
		}
		finally {
			table.close();
			redisUtils.returnResource(jedis);
		}
		return object.toString();
	}
	
	
	
	 public static String reverse1(String s) {  
	        int length = s.length();  
	        if (length <= 1)  
	            return s;  
	        String left = s.substring(0, length / 2);  
	        String right = s.substring(length / 2, length);  
	        return reverse1(right) + reverse1(left);  
	    }  
	
	
	
//	public static void main(String[] args) throws Exception {
//		
//
//		
//		String input = "user=bdms2001&pwd=bdms200120170420&channelId=2&actionID=201601010003&modelID=8109990003&qryUserProRuleId=104000001&targetTypeId=1&prvcID=854&targetObjectId=18911951973";//18911951973   //13308900000
//		String result = resolve_json(input);
//	    System.out.println(result);
//
//	}
}

