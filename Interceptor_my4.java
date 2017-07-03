package service_logs;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.common.collect.Lists;

//10.140.16.249 - - [06/Jun/2017:15:09:17 +0800] "GET /qryUserPro?targetObjectId=18105125395&modelID=8109990007&prvcID=832&qryUserProRuleId=104000001&targetTypeId=1&actionID=201501010007&pwd=bdms000120170412&user=bdms0001&channelId=4 HTTP/1.1" 200 19 "-" "Java/1.8.0_111" "10.250.100.8" 0.000 
/**
 * 
 * @author Administrator
 *
���Ǵ�kafka��hdfs��������  ͬʱ������������  ��һ���� ����Ϣ�����˸�ʽ������һ�������Ƕ�  ����Ϣ�е� ���ڼ����� ·�� ��  �ļ�����
���ذ�flumeװ��kafka���ڵĽڵ�  ��ֻҪ�ڼ�Ⱥ�е���һ���Ϳ��ԣ���Ϊflume���õ���zk������



[ctgmkt@BQJ-EDA-XH628-2015030 conf]$ vi kafka-hdfs.conf

aaaa.sources = kafkaSource
aaaa.channels = memoryChannel
aaaa.sinks = hdfsSink

aaaa.sources.kafkaSource.channels = memoryChannel
aaaa.sinks.hdfsSink.channel = memoryChannel


aaaa.sources.kafkaSource.type = org.apache.flume.source.kafka.KafkaSource
aaaa.sources.kafkaSource.zookeeperConnect = 10.140.16.201:2181/mktkafka
aaaa.sources.kafkaSource.topic = tpnginxlog
aaaa.sources.kafkaSource.groupId = flume_1
aaaa.sources.kafkaSource.kafka.consumer.timeout.ms = 100




aaaa.channels.memoryChannel.type = memory
aaaa.channels.memoryChannel.capacity=10000
aaaa.channels.memoryChannel.transactionCapacity=5000




aaaa.sources.kafkaSource.interceptors = i1
aaaa.sources.kafkaSource.interceptors.i1.type = service_logs.Interceptor_my4$Builder
aaaa.sources.kafkaSource.interceptors.i1.preserveExisting = false


#aaaa.sinks.hdfsSink.type=logger

aaaa.sinks.hdfsSink.type=hdfs
aaaa.sinks.hdfsSink.hdfs.path=hdfs://nsctgmkt/yhl/test/flume/logs32/%{time_day}   #���������н�ȡ�������ݷ��뵽����
aaaa.sinks.hdfsSink.hdfs.filePrefix=logFile.%{time_hour}
aaaa.sinks.hdfsSink.hdfs.fileSuffix=.log
aaaa.sinks.hdfsSink.hdfs.idleTimeout=60000
aaaa.sinks.sink1.hdfs.round = true

aaaa.sinks.hdfsSink.hdfs.rollInterval = 7200   #ÿ����Сʱ�ع���һ���ļ�

aaaa.sinks.hdfsSink.hdfs.rollSize = 128000000
aaaa.sinks.hdfsSink.hdfs.rollCount = 0
aaaa.sinks.hdfsSink.hdfs.batchSize = 1000


aaaa.sinks.hdfsSink.hdfs.roundValue = 1
aaaa.sinks.hdfsSink.hdfs.roundUnit = minute
aaaa.sinks.hdfsSink.hdfs.useLocalTimeStamp = false     #Ҫ���ó�false�Ż����־�л�ȡʱ�䣬�����Ǵ�������ϵͳʱ���ȡ
aaaa.sinks.hdfsSink.memoryChannel=fileChannel
aaaa.sinks.hdfsSink.hdfs.fileType = DataStream




 *
 *
 *
 *
 *
 *
 */




public class Interceptor_my4 implements Interceptor {

	  private static final SimpleDateFormat fm_d = new SimpleDateFormat("yyyyMMdd");
	  private static final SimpleDateFormat fm_h = new SimpleDateFormat("yyyyMMddHH");
	  private static final SimpleDateFormat fm_s = new SimpleDateFormat("yyyyMMddHHmmss");
	  private static final SimpleDateFormat fm_e = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
	  
	  
	
	
	
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Event intercept(Event even) {
		Map<String,String> header = even.getHeaders();

		
		
		
		
		
		
		
		
		
		String str = "";
		Map<String,String> treeMap = new TreeMap<String,String>();
		
		String body = new String(even.getBody());
		
			
		if(body.contains("qryUserProRuleId")){
			
			String time = body.split("\\]")[0].split("\\[")[1].split(" ")[0];
			String time_hour = null;
			String time_day = null;
			String time_sec = null;
			try {
				time_hour = fm_h.format(fm_e.parse(time));
				time_day = fm_d.format(fm_e.parse(time));
				time_sec = fm_s.format(fm_e.parse(time));
				
				
			
				
				header.put("time_hour", time_hour);
				header.put("time_day", time_day);
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
			
			

			String[] arr = body.split("\\[");
			if(arr.length > 1){
				String content1 = arr[1].split(" HTTP")[0];
				String[] arr1  = content1.split("\\?");
				if(arr1.length > 1){
					 String content2 = arr1[1];
					 String[] strs = content2.split("&");
				        for( String e : strs){
				            if(e.contains("=")){
					          String[] ee = e.split("=");
					          treeMap.put(ee[0], ee[1]);
					        }
				        }
				      Iterator<String> it = treeMap.keySet().iterator();
				      while(it.hasNext()){
				        String key = it.next();
				        String v = treeMap.get(key);
				        str += v+",";
				      }
				      str +=time_sec;
				     
				      
				      
				      
				      even.setBody(str.getBytes());
				}
			}else{
				String content3 = new String(even.getBody());
				even.setBody(content3.getBytes());
			}
		}else{
			 even.setBody(null);
		}
		
		return even;
		
		
		
		/**
		 * 07/Jun/2017:11:16:48 +0800] "GET /qryUserPro?targetObjectId=15326896296&modelID=8109990007&prvcID=815&qryUserProRuleId=104000001&targetTypeId=1&actionID=201501010007&pwd=bdms000120170412&user=bdms0001&channelId=4
		 */
		
	
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());  
		for (Event event : events) {
			
			Event interceptedEvent = intercept(event);
			String body = new String(interceptedEvent.getBody());
			if(body != null && body.length() != 0 && !body.equals("")){
				intercepted.add(interceptedEvent);
			}
		}
		return intercepted;
	}
	
	
	  
			public static class Builder implements Interceptor.Builder {

				

				@Override
				public Interceptor build() {
					return new Interceptor_my4();
				}

				@Override
				public void configure(Context context) {
				
				}
			}

	
}
