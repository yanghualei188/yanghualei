package hbase_service;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

public class UserFeatures {
	
	//总汇信息
	public static String totalInfo(String iphoneNum) throws Exception{
		JSONObject jo_total = new JSONObject();

		Table mkt_portrait_info = null;
		JSONArray ja_1_baseInfo = new JSONArray();
		JSONArray ja_2_baseInfo = new JSONArray();
		JSONObject jo_baseInfo = new JSONObject();
		try{
			mkt_portrait_info = HbaseUtils.mkt_portrait_info();
			Get get = new Get(Bytes.toBytes(iphoneNum));
			Result result = mkt_portrait_info.get(get);

			//baseInfo   客户基本信息
			LinkedHashMap<String,String> map_baseInfo = Utils.field_baseInfo();
			Iterator<String> iterator_baseInfo = map_baseInfo.keySet().iterator();
			while(iterator_baseInfo.hasNext()){
				JSONObject jo_inner_1_baseInfo = new JSONObject();
				JSONObject jo_inner_2_baseInfo = new JSONObject();
				String key =  iterator_baseInfo.next();
				String value = map_baseInfo.get(key);
				jo_inner_1_baseInfo.put("name", value +":"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(key))));
				jo_inner_1_baseInfo.put("category","1");
				ja_1_baseInfo.add(jo_inner_1_baseInfo);
				
				jo_inner_2_baseInfo.put("weight","5");
				jo_inner_2_baseInfo.put("target",Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME"))));
				jo_inner_2_baseInfo.put("source", value +":"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(key))));
				ja_2_baseInfo.add(jo_inner_2_baseInfo);
			}
			jo_baseInfo.put("nodes", ja_1_baseInfo);
			jo_baseInfo.put("links", ja_2_baseInfo);
			jo_baseInfo.put("target", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME"))));
			jo_baseInfo.put("sex", "性别:"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("GENDER"))));
			jo_baseInfo.put("prvnce_name", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("PRVNCE_NAME"))));
			jo_baseInfo.put("latn_name", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("LATN_NAME"))));
			jo_total.put("baseInfo", jo_baseInfo);
			
			// credietInfo 征信信息
			JSONArray ja_credietInfo = new JSONArray();
			JSONObject jo_credietInfo = new JSONObject();

			JSONObject jo_inner_1_credietInfo = new JSONObject();
			jo_inner_1_credietInfo.put("text", "橙信分");
			jo_inner_1_credietInfo.put("max", "999");
			

			JSONObject jo_inner_2_credietInfo = new JSONObject();
			jo_inner_2_credietInfo.put("text", "橙信分等级");
			jo_inner_2_credietInfo.put("max", "5");
			 
			
			ja_credietInfo.add(jo_inner_1_credietInfo);
			ja_credietInfo.add(jo_inner_2_credietInfo);
			
			jo_credietInfo.put("indicator", ja_credietInfo);
			
			
			JSONArray ja_1_credietInfo = new JSONArray();
			ja_1_credietInfo.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("COMP_SCORE"))));
			ja_1_credietInfo.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("COMP_SCORE_GRADE"))));
			
			jo_credietInfo.put("value", ja_1_credietInfo);
		
		
			jo_total.put("creditInfo", jo_credietInfo);
			
			//应用排行   AppSort
			JSONArray ja_1_AppSort = new JSONArray();
			JSONArray ja_2_AppSort = new JSONArray();
			JSONObject jo_AppSort = new JSONObject();

				
			ja_1_AppSort.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_ONE"))));
			ja_1_AppSort.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_TWO"))));
			ja_1_AppSort.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_THREE"))));
			ja_1_AppSort.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_FOUR"))));
			ja_1_AppSort.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("APP_TOP_FIVE"))));
			
			ja_2_AppSort.add("5");
			ja_2_AppSort.add("4");
			ja_2_AppSort.add("3");
			ja_2_AppSort.add("2");
			ja_2_AppSort.add("1");
			
			jo_AppSort.put("app", ja_1_AppSort);
			jo_AppSort.put("value", ja_2_AppSort);
		
			jo_total.put("AppSort", jo_AppSort);
			
			//terminalChange  终端换机
			JSONArray ja_terminalChange = new JSONArray();
			JSONObject jo_terminalChange = new JSONObject();

			List<String> list_terminalChange = Utils.field_terminalChange();
			
			String time = "";
			String model = "";
			List<String> list_time = new ArrayList<String>(); 
			List<String> list_model = new ArrayList<String>(); 

			
			for(String eString : list_terminalChange){
				
				
				if(eString.contains("RGST_DT")){
					time = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(eString)));
					if(time.length() > 0){			
						list_time.add(time);
					}
				}else{
					model = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(eString)));	
					if(model.length() > 0){	
						list_model.add(model);
					}
				}
			}
			

			
	
			for(int i = 0 ; i < list_model.size() ; i++){
				JSONObject jo_inner_terminalChange = new JSONObject();

				jo_inner_terminalChange.put("time",list_time.get(i));
				jo_inner_terminalChange.put("model",list_model.get(i));
				ja_terminalChange.add(jo_inner_terminalChange);
			}
			
			
				
				
			
			
			jo_terminalChange.put("phone", ja_terminalChange);
			
			jo_total.put("terminalChange", jo_terminalChange);	

			
			//位置   position
			
			JSONObject jo_position = new JSONObject();

			jo_position.put("workLoc", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("WORKING_PLACE"))).replace(":", ""));
			jo_position.put("homeLoc", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("LIVE_PLACE"))).replace(":", ""));
				
			jo_total.put("position", jo_position);	
			
			//营销建议  marketingSuggestion
			JSONArray ja_marketingSuggestion = new JSONArray();
			JSONArray ja_mkSuggest_marketingSuggestion = new JSONArray();
			JSONArray ja_keepSuggest_marketingSuggestion = new JSONArray();
			JSONObject jo_marketingSuggestion = new JSONObject();
	
			Map<String,String> map_marketingSuggestion = Utils.field_marketingSuggestion();
			Iterator<String> iterator_marketingSuggestion = map_marketingSuggestion.keySet().iterator();
			while(iterator_marketingSuggestion.hasNext()){
				JSONObject jo_inner_marketingSuggestion = new JSONObject();
				String key =  iterator_marketingSuggestion.next();
				String value = map_marketingSuggestion.get(key);
				String hbaseValue_marketingSuggestion = Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(value)));
				if(hbaseValue_marketingSuggestion.length() > 0){
					jo_inner_marketingSuggestion.put("value", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes(value))));
					jo_inner_marketingSuggestion.put("name",key);
					jo_inner_marketingSuggestion.put("category","1");
				}
				
				ja_marketingSuggestion.add(jo_inner_marketingSuggestion);
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
					
					ja_mkSuggest_marketingSuggestion.add(jo_inner_mkSuggest);
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
				
//				list_keepSuggest_1.add(keepSuggest_brith_2);
				list_keepSuggest_1.add("通过短信进行生日关怀");
				list_keepSuggest_1.add(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_FIVE"))));
				
				for(int i = 0 ; i < 2; i++){
					JSONObject jo_inner_keepSuggest = new JSONObject();
					jo_inner_keepSuggest.put("value",list_keepSuggest_1.get(i));
					jo_inner_keepSuggest.put("name",queue_keepSuggest.poll());
					jo_inner_keepSuggest.put("category","1");
					ja_keepSuggest_marketingSuggestion.add(jo_inner_keepSuggest);
				}
			}else{
				if(Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_FIVE"))).length() > 0){
					JSONObject jo_inner_keepSuggest = new JSONObject();
					jo_inner_keepSuggest.put("value",Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MKT_SUGGEST_FIVE"))));
					jo_inner_keepSuggest.put("name","维系建议一");
					jo_inner_keepSuggest.put("category","1");
					ja_keepSuggest_marketingSuggestion.add(jo_inner_keepSuggest);
				}else{
					
				}
			
			}
			
			
			jo_marketingSuggestion.put("name", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CUST_NAME"))));
			jo_marketingSuggestion.put("values", ja_marketingSuggestion);
			jo_marketingSuggestion.put("successTimes", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("MARKT_SUCCESS_TIMES"))));

			if(ja_mkSuggest_marketingSuggestion.size() == 0 && ja_keepSuggest_marketingSuggestion.size() == 0){
				jo_marketingSuggestion.put("mkSuggest", "通过"+Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("PO_CHANNEL_SLL_TYPE")))+"推荐优惠活动");
			}else if(ja_mkSuggest_marketingSuggestion.size() != 0 && ja_keepSuggest_marketingSuggestion.size() == 0){
				jo_marketingSuggestion.put("mkSuggest", ja_mkSuggest_marketingSuggestion);
			}else if(ja_mkSuggest_marketingSuggestion.size() == 0 && ja_keepSuggest_marketingSuggestion.size() != 0){
				jo_marketingSuggestion.put("keepSuggest", ja_keepSuggest_marketingSuggestion);
			}else if(ja_mkSuggest_marketingSuggestion.size() != 0 && ja_keepSuggest_marketingSuggestion.size() != 0){
				jo_marketingSuggestion.put("mkSuggest", ja_mkSuggest_marketingSuggestion);
				jo_marketingSuggestion.put("keepSuggest", ja_keepSuggest_marketingSuggestion);
			}
			
			
			jo_total.put("marketingSuggestion", jo_marketingSuggestion);	

			//特征信息  family  job  social   
			Table mkt_portrait_feature_dimension = null;
			JSONObject jo_featuresInfo = new JSONObject();
		
			mkt_portrait_feature_dimension = HbaseUtils.mkt_portrait_feature_dimension();

	
			Get get_picture_family = new Get(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("FAMILY_GROUPING")));
			Get get_picture_occu = new Get(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("OCCUPATIONAL_GROUPING")));
			Get get_picture_social = new Get(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("SOCIAL_GROUPING")));
			
			Result result_family = mkt_portrait_feature_dimension.get(get_picture_family);
			Result result_picture_occu = mkt_portrait_feature_dimension.get(get_picture_occu);
			Result result_picture_social = mkt_portrait_feature_dimension.get(get_picture_social);

			jo_featuresInfo.put("familyName", Bytes.toString(result_family.getValue(Bytes.toBytes("feature"), Bytes.toBytes("name"))));
			jo_featuresInfo.put("jobName", Bytes.toString(result_picture_occu.getValue(Bytes.toBytes("feature"), Bytes.toBytes("name"))));
			jo_featuresInfo.put("socialName", Bytes.toString(result_picture_social.getValue(Bytes.toBytes("feature"), Bytes.toBytes("name"))));

			jo_featuresInfo.put("familyImage", Bytes.toString(result_family.getValue(Bytes.toBytes("feature"), Bytes.toBytes("image"))));
			jo_featuresInfo.put("jobImage", Bytes.toString(result_picture_occu.getValue(Bytes.toBytes("feature"), Bytes.toBytes("image"))));
			jo_featuresInfo.put("socialImage", Bytes.toString(result_picture_social.getValue(Bytes.toBytes("feature"), Bytes.toBytes("image"))));

			jo_total.put("featuresInfo", jo_featuresInfo);	
			
			
			//消费情况   consumeInfo

			Table mkt_portrait_trail = null;
			JSONArray ja_lastYear_consumeInfo = new JSONArray();
			JSONArray ja_thisYear_consumeInfo = new JSONArray();
			JSONObject jo_consumeInfo = new JSONObject();

			mkt_portrait_trail = HbaseUtils.mkt_portrait_trail();
			List<String> list_consumeInfo = Utils.getNearDate();
			//取去年的12个月 倒序的 从201601 开始的
			for(int i = list_consumeInfo.size()-1 ; i >=  list_consumeInfo.size()-12 ;i--){
				Get get_consumeInfo = new Get(Bytes.toBytes(iphoneNum+list_consumeInfo.get(i)));
				Result result_consumeInfo = mkt_portrait_trail.get(get_consumeInfo);
				String charge = Bytes.toString(result_consumeInfo.getValue(Bytes.toBytes("label"), Bytes.toBytes("BILL_CHARGE")));
				ja_lastYear_consumeInfo.add(charge);
			}

			//取今年的几个月
			for(int i = list_consumeInfo.size()-13 ; i >= 1 ;i--){
				Get get_consumeInfo = new Get(Bytes.toBytes(iphoneNum+list_consumeInfo.get(i)));
				Result result_consumeInfo = mkt_portrait_trail.get(get_consumeInfo);
				String charge = Bytes.toString(result_consumeInfo.getValue(Bytes.toBytes("label"), Bytes.toBytes("BILL_CHARGE")));
				ja_thisYear_consumeInfo.add(charge);
			}
			
			jo_consumeInfo.put("charge_analysed", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("CHARGE_ANALYSED"))));
			jo_consumeInfo.put("thisYear", ja_thisYear_consumeInfo);
			jo_consumeInfo.put("lastYear", ja_lastYear_consumeInfo);

			jo_total.put("consumeInfo", jo_consumeInfo);	
			
			//流量使用情况  flowInfo
			
			
			JSONArray ja_lastYear_flowInfo = new JSONArray();
			JSONArray ja_thisYear_flowInfo = new JSONArray();
			JSONObject jo_flowInfo = new JSONObject();

			List<String> list_flowInfo = Utils.getNearDate();
			//取去年的12个月 倒序的 从201601 开始的
			for(int i = list_flowInfo.size()-1 ; i >=  list_flowInfo.size()-12 ;i--){
				Get get_flowInfo = new Get(Bytes.toBytes(iphoneNum+list_flowInfo.get(i)));
				Result result_flowInfo = mkt_portrait_trail.get(get_flowInfo);
				String charge_flowInfo = Bytes.toString(result_flowInfo.getValue(Bytes.toBytes("label"), Bytes.toBytes("MBL_INNET_FLUX")));
				ja_lastYear_flowInfo.add(charge_flowInfo);
			}

			//取今年的几个月
			for(int i = list_flowInfo.size()-13 ; i >= 1 ;i--){
				Get get_flowInfo = new Get(Bytes.toBytes(iphoneNum+list_flowInfo.get(i)));
				Result result_flowInfo = mkt_portrait_trail.get(get_flowInfo);
				String charge_flowInfo = Bytes.toString(result_flowInfo.getValue(Bytes.toBytes("label"), Bytes.toBytes("MBL_INNET_FLUX")));
				ja_thisYear_flowInfo.add(charge_flowInfo);
			}
			
			jo_flowInfo.put("flux_analysed", Bytes.toString(result.getValue(Bytes.toBytes("label"), Bytes.toBytes("FLUX_ANALYSED"))));
			jo_flowInfo.put("thisYear", ja_thisYear_flowInfo);
			jo_flowInfo.put("lastYear", ja_lastYear_flowInfo);
		
			jo_total.put("flowInfo", jo_flowInfo);	
		
			
			
			mkt_portrait_trail.close();
			mkt_portrait_feature_dimension.close();
			mkt_portrait_info.close();
			//关闭连接池子  HConnection
//			HbaseUtils2.closeTable(mkt_portrait_info);
			
			return jo_total.toString();
		}catch(Exception e){
			throw e;
		}
	}
	
	

	
	

	
	
	
	
	
	
	
	
	
	
//	public static void main(String[] args) throws Exception {
////////		System.out.println(baseInfo("18031752422"));
////		System.out.println(creditInfo("18031752422"));
////////		System.out.println(AppSort("18031752422"));
////////		System.out.println(terminalChange("18031752422"));
//////		System.out.println(marketingSuggestion("18031752422"));
////////		System.out.println(position("18031752422"));
//////	
////////		System.out.println(featuresInfo("18031752422"));
//////		
//////
////////		System.out.println(consumeInfo("18031752422"));
////////		System.out.println(flowInfo("18031752422"));
//////		
//////
//////
//		System.out.println(totalInfo("13304264153"));//       13378915688     18173127979   13304264153
//	}

}
