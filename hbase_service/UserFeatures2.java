package hbase_service;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class UserFeatures2 {
	//基本信息 
	public static String baseInfo(String iphoneNum) throws Exception{
		Table mkt_portrait_info = null;
		JSONArray ja_1 = new JSONArray();
		JSONArray ja_2 = new JSONArray();
		JSONObject jo = new JSONObject();
		try{
			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);

			Map<String,String> map = Utils.field_baseInfo();
			 

			Iterator<String> iterator = map.keySet().iterator();
			while(iterator.hasNext()){
				JSONObject jo_inner_1 = new JSONObject();
				JSONObject jo_inner_2 = new JSONObject();
				String key =  iterator.next();
				String value = map.get(key);
				jo_inner_1.put("name", value +":"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(key))));
				jo_inner_1.put("category","1");
				ja_1.add(jo_inner_1);
				
				jo_inner_2.put("weight","5");
				jo_inner_2.put("target",Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME"))));
				jo_inner_2.put("source", value +":"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(key))));
				ja_2.add(jo_inner_2);
			}
			 
			 
		 
			jo.put("nodes", ja_1);
			jo.put("links", ja_2);
			jo.put("target", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME"))));
			jo.put("sex", "性别:"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("GENDER"))));

			mkt_portrait_info.close();
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
	}
	//征信信息
	public static String creditInfo(String iphoneNum) throws Exception{
		Table mkt_portrait_info = null;
		JSONArray ja = new JSONArray();
		JSONObject jo = new JSONObject();
		try{

			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);
			
		
			JSONObject jo_inner_1 = new JSONObject();
			jo_inner_1.put("text", "橙信分");
			jo_inner_1.put("max", "999");
			

			JSONObject jo_inner_2 = new JSONObject();
			jo_inner_2.put("text", "橙信分等级");
			jo_inner_2.put("max", "5");
			 
			
			ja.add(jo_inner_1);
			ja.add(jo_inner_2);
			
			jo.put("indicator", ja);
			
			
			JSONArray ja_1 = new JSONArray();
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("COMP_SCORE"))));
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("COMP_SCORE_GRADE"))));
			
			jo.put("value", ja_1);
			
			mkt_portrait_info.close();
			return jo.toString();
		
		}catch(Exception e){
			throw e;
		}
	}
	//应用排行
	public static String AppSort(String iphoneNum) throws Exception{
		Table mkt_portrait_info = null;
		JSONArray ja_1 = new JSONArray();
		JSONArray ja_2 = new JSONArray();
		JSONObject jo = new JSONObject();
		try{
			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);
			
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_ONE"))));
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_TWO"))));
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_THREE"))));
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_FOUR"))));
			ja_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_FIVE"))));
			
			ja_2.add("5");
			ja_2.add("4");
			ja_2.add("3");
			ja_2.add("2");
			ja_2.add("1");
			
			jo.put("app", ja_1);
			jo.put("value", ja_2);
			
			mkt_portrait_info.close();
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
	}
	//消费情况 
	public static String consumeInfo(String iphoneNum) throws Exception{
		
		Table mkt_portrait_trail = null;
		JSONArray ja_lastYear = new JSONArray();
		JSONArray ja_thisYear = new JSONArray();
		JSONObject jo = new JSONObject();
		try{
			
			mkt_portrait_trail = HbaseUtils.mkt_portrait_trail();
			List<String> list = Utils.getNearDate();
			//取去年的12个月 倒序的 从201601 开始的
			for(int i = list.size()-1 ; i >=  list.size()-12 ;i--){
				Get get = new Get(Bytes.toBytes(iphoneNum+list.get(i)));
				Result result = mkt_portrait_trail.get(get);
				String charge = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("BILL_CHARGE")));
				ja_lastYear.add(charge);
			}
			
			
			
			//取今年的几个月
			for(int i = list.size()-13 ; i >= 0 ;i--){
				Get get = new Get(Bytes.toBytes(iphoneNum+list.get(i)));
				Result result = mkt_portrait_trail.get(get);
				String charge = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("BILL_CHARGE")));
				ja_thisYear.add(charge);
			}
			
			
			jo.put("thisYear", ja_thisYear);
			jo.put("lastYear", ja_lastYear);
			
			mkt_portrait_trail.close();
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
		
	}
	
	
	//流量使用情况
	public static String flowInfo(String iphoneNum) throws Exception{

		Table mkt_portrait_trail = null;
		JSONArray ja_lastYear = new JSONArray();
		JSONArray ja_thisYear = new JSONArray();
		JSONObject jo = new JSONObject();
		try{
			
			mkt_portrait_trail = HbaseUtils.mkt_portrait_trail();
			List<String> list = Utils.getNearDate();
			//取去年的12个月 倒序的 从201601 开始的
			for(int i = list.size()-1 ; i >=  list.size()-12 ;i--){
				Get get = new Get(Bytes.toBytes(iphoneNum+list.get(i)));
				Result result = mkt_portrait_trail.get(get);
				String charge = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MBL_INNET_FLUX")));
				ja_lastYear.add(charge);
			}
			
			
			
			//取今年的几个月
			for(int i = list.size()-13 ; i >= 1 ;i--){
				Get get = new Get(Bytes.toBytes(iphoneNum+list.get(i)));
				Result result = mkt_portrait_trail.get(get);
				String charge = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MBL_INNET_FLUX")));
				ja_thisYear.add(charge);
			}
			
			
			jo.put("thisYear", ja_thisYear);
			jo.put("lastYear", ja_lastYear);
			
			mkt_portrait_trail.close();
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
	}
	
	
	//终端换机
//	public static String terminalChange(String iphoneNum) throws Exception{
//		Table mkt_portrait_info = null;
//		JSONArray ja = new JSONArray();
//		JSONObject jo = new JSONObject();
//		try{
//			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
//			Get get = new Get(Bytes.toBytes(iphoneNum));
//			Result result = mkt_portrait_info.get(get);
//			Map<String,String> map = Utils.field_terminalChange();
//			Iterator<String> iterator = map.keySet().iterator();
//			String time = "";
//			String model = "";
//			while(iterator.hasNext()){
//				JSONObject jo_inner = new JSONObject();
//				String key =  iterator.next();
//				if(map.get(key).contains("RGST_DT")){
//					time = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(map.get(key))));
//				}else{
//					model = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(map.get(key))));	
//				}
//				
//				
//				
//				if(time.length() > 0 && model.length() > 0 ){
//					jo_inner.put("time",time);
//					jo_inner.put("model",model);
//					ja.add(jo_inner);
//				}
//				
//			}
//			
//	
//			jo.put("phone", ja);
//			mkt_portrait_info.close();
//			return jo.toString();
//		}catch(Exception e){
//			throw e;
//		}
//	}
	
	/**
	 *  营销建议描述(营销话术)1,MKT_SUGGEST_ONE
		营销建议描述(营销话术)2,MKT_SUGGEST_TWO
		营销建议描述(营销话术)3,MKT_SUGGEST_THREE
		营销建议描述(营销话术)4,MKT_SUGGEST_FOUR
	 */
	
	
	
	//营销建议
	public static String marketingSuggestion(String iphoneNum) throws Exception{
		Table mkt_portrait_info = null;
		JSONArray ja = new JSONArray();
		JSONArray ja_mkSuggest = new JSONArray();
		JSONArray ja_keepSuggest = new JSONArray();
		JSONObject jo = new JSONObject();
		try{
			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);
			Map<String,String> map = Utils.field_marketingSuggestion();
			Iterator<String> iterator = map.keySet().iterator();
			while(iterator.hasNext()){
				JSONObject jo_inner = new JSONObject();
				String key =  iterator.next();
				String value = map.get(key);
				String hbaseValue = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(value)));
				if(hbaseValue.length() > 0){
					jo_inner.put("value", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(value))));
					jo_inner.put("name",key);
					jo_inner.put("category","1");
				}
				
				ja.add(jo_inner);
			}
			

			
			//建立两个list
			Queue<String> queue_mkSuggest = new LinkedList<String>();
			queue_mkSuggest.offer("营销建议一");
			queue_mkSuggest.offer("营销建议二");
			queue_mkSuggest.offer("营销建议三");
			
			
			List<String> list_mkSuggest_k = new ArrayList<String>();
			list_mkSuggest_k.add("MKT_SUGGEST_TWO");
			list_mkSuggest_k.add("MKT_SUGGEST_THREE");
			list_mkSuggest_k.add("MKT_SUGGEST_FOUR");
			
			
			//营销建议
			String mkSuggest_v = "";
			for(int i = 0 ; i < list_mkSuggest_k.size();i++){
				mkSuggest_v = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(list_mkSuggest_k.get(i))));
				if(mkSuggest_v.length() > 0){
					JSONObject jo_inner_mkSuggest = new JSONObject();
					
					jo_inner_mkSuggest.put("value",mkSuggest_v);
					jo_inner_mkSuggest.put("name",queue_mkSuggest.poll());
					jo_inner_mkSuggest.put("category","1");
					
					ja_mkSuggest.add(jo_inner_mkSuggest);
				}
			}
			
			//维系建议
			
			
			Queue<String> queue_keepSuggest = new LinkedList<String>();
			queue_keepSuggest.add("维系建议一");
			queue_keepSuggest.add("维系建议二");
			
			List<String> list_keepSuggest_1 = new ArrayList<String>();
			
			
			
			String keepSuggest_brith_2 = "";
			String keepSuggest_brith_1 = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_ONE")));
			SimpleDateFormat sdf = new SimpleDateFormat("MMdd");
			String curMonthDay  = sdf.format(System.currentTimeMillis());
			
			if(curMonthDay.equals(keepSuggest_brith_1)){
				keepSuggest_brith_2 = keepSuggest_brith_1;
			}
			
			if(keepSuggest_brith_2.length() > 0 ){
				
				list_keepSuggest_1.add(keepSuggest_brith_2);
				list_keepSuggest_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_FIVE"))));
				
				for(int i = 0 ; i < 2; i++){
					JSONObject jo_inner_keepSuggest = new JSONObject();
					jo_inner_keepSuggest.put("value",list_keepSuggest_1.get(i));
					jo_inner_keepSuggest.put("name",queue_keepSuggest.poll());
					jo_inner_keepSuggest.put("category","1");
					ja_keepSuggest.add(jo_inner_keepSuggest);
				}
			}else{
				if(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_FIVE"))).length() > 0){
					JSONObject jo_inner_keepSuggest = new JSONObject();
					jo_inner_keepSuggest.put("value",Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_FIVE"))));
					jo_inner_keepSuggest.put("name","维系建议一");
					jo_inner_keepSuggest.put("category","1");
					ja_keepSuggest.add(jo_inner_keepSuggest);
				}else{
					
				}
			
			}
			
			
			jo.put("name", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME"))));
			jo.put("values", ja);
			jo.put("successTimes", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MARKT_SUCCESS_TIMES"))));

			if(ja_mkSuggest.size() == 0 && ja_keepSuggest.size() == 0){
				jo.put("mkSuggest", "通过"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("PO_CHANNEL_SLL_TYPE")))+"推荐优惠活动");
			}else if(ja_mkSuggest.size() != 0 || ja_keepSuggest.size() == 0){
				jo.put("mkSuggest", ja_mkSuggest);
			}else if(ja_mkSuggest.size() == 0 || ja_keepSuggest.size() != 0){
				jo.put("keepSuggest", ja_keepSuggest);
			}

			
			
			mkt_portrait_info.close();
			
//			HbaseUtils2.closeTable(mkt_portrait_info);
			
			
			
				
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
	}

	//位置
	public static String position(String iphoneNum) throws Exception{
		Table mkt_portrait_info = null;
	
		JSONObject jo = new JSONObject();
		try{
			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);
			
			jo.put("workLoc", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("WORKING_PLACE"))).replace(":", ""));
			jo.put("homeLoc", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("LIVE_PLACE"))).replace(":", ""));

			mkt_portrait_info.close();
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
	}
	//特征信息  family  job  social
	public static String featuresInfo(String iphoneNum) throws Exception{
		Table mkt_portrait_info = null;
		Table mkt_portrait_feature_dimension = null;
		JSONObject jo = new JSONObject();
		try{
			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			mkt_portrait_feature_dimension = HbaseUtils.mkt_portrait_feature_dimension();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);
//			String cust_name = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME")));
	
			Get get_picture_family = new Get(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("FAMILY_GROUPING")));
			Get get_picture_occu = new Get(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("OCCUPATIONAL_GROUPING")));
			Get get_picture_social = new Get(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("SOCIAL_GROUPING")));
			
			Result result_family = mkt_portrait_feature_dimension.get(get_picture_family);
			Result result_picture_occu = mkt_portrait_feature_dimension.get(get_picture_occu);
			Result result_picture_social = mkt_portrait_feature_dimension.get(get_picture_social);
			
			
			
			jo.put("familyName", Bytes.toString(result_family.getValue(Bytes.toBytes("feature"), Bytes.toBytes("name"))));
			jo.put("jobName", Bytes.toString(result_picture_occu.getValue(Bytes.toBytes("feature"), Bytes.toBytes("name"))));
			jo.put("socialName", Bytes.toString(result_picture_social.getValue(Bytes.toBytes("feature"), Bytes.toBytes("name"))));
			
			
			jo.put("familyImage", Bytes.toString(result_family.getValue(Bytes.toBytes("feature"), Bytes.toBytes("image"))));
			jo.put("jobImage", Bytes.toString(result_picture_occu.getValue(Bytes.toBytes("feature"), Bytes.toBytes("image"))));
			jo.put("socialImage", Bytes.toString(result_picture_social.getValue(Bytes.toBytes("feature"), Bytes.toBytes("image"))));
			
			mkt_portrait_feature_dimension.close();
			mkt_portrait_info.close();
			return jo.toString();
		}catch(Exception e){
			throw e;
		}
	}
	
	
	
	
	
	
	
	
	
	
	
//	public static void main(String[] args) throws Exception {
//////		System.out.println(baseInfo("18031752422"));
//////		System.out.println(creditInfo("18031752422"));
//////		System.out.println(AppSort("18031752422"));
//////		System.out.println(terminalChange("18031752422"));
////		System.out.println(marketingSuggestion("18031752422"));
//////		System.out.println(position("18031752422"));
////	
//////		System.out.println(featuresInfo("18031752422"));
////		
////
//////		System.out.println(consumeInfo("18031752422"));
//////		System.out.println(flowInfo("18031752422"));
////		
////
////
//	}

}
