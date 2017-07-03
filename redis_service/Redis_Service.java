package redis_service;



import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;


public class Redis_Service {
	//[{"a":"15458","m":"15458"},{"m1":"15458","a1":"15458"}]
	public static String fiveSeccons(){
		SimpleDateFormat fm = new SimpleDateFormat("yyyyMM");
		String month = fm.format(System.currentTimeMillis());
		JSONObject jo_a = new JSONObject();
		JSONObject jo_m = new JSONObject();
		JSONObject jo_i_m = new JSONObject();
		JSONArray ja = new JSONArray();
		JSONObject jo = new JSONObject();
		Jedis jedis = null;
		try{
			jedis = redisUtils.pool.getResource();
			String a = jedis.hget("MKTstreaming_a","a");
			String m = jedis.hget("MKTstreaming_m",month);
			jo_a.put("a", a);
			jo_m.put("m", m);
			ja.add(jo_a);
			ja.add(jo_m);
			List<String> i_m = jedis.hmget("MKTstreaming_i_m","streaming_104000001_"+month,"streaming_102000001_"+month,"streaming_103000001_"+month);
			for(int i = 0 ; i < i_m.size();i++){
				jo_i_m.put("i_m_"+i, i_m.get(i));
			}
			ja.add(jo_i_m);
			jo.put("5Sec", ja);
			return jo.toString();
		}catch(Exception e){
			throw e;
		}finally {
			redisUtils.returnResource(jedis);
		}
	}




	public static String oneHour(){
		SimpleDateFormat fm = new SimpleDateFormat("yyyyMM");
		String month = fm.format(System.currentTimeMillis());
		JSONArray ja_p_m = new JSONArray();
		JSONObject jo = new JSONObject();
		Jedis jedis = null;
		try{
			jedis = redisUtils.pool.getResource();
			List<String> p_m = jedis.hmget(
					"MKTstreaming_p_m",
					"streaming_811_"+month,"streaming_812_"+month,"streaming_813_"+month,"streaming_814_"+month,
					"streaming_815_"+month,"streaming_821_"+month,"streaming_822_"+month,"streaming_823_"+month,"streaming_831_"+month,
					"streaming_832_"+month,"streaming_833_"+month,"streaming_834_"+month,"streaming_835_"+month,"streaming_836_"+month,
					"streaming_837_"+month,"streaming_841_"+month,"streaming_842_"+month,"streaming_843_"+month,"streaming_844_"+month,
					"streaming_845_"+month,"streaming_846_"+month,"streaming_850_"+month,"streaming_851_"+month,"streaming_852_"+month,
					"streaming_853_"+month,"streaming_854_"+month,"streaming_861_"+month,"streaming_862_"+month,"streaming_863_"+month,
					"streaming_864_"+month,"streaming_865_"+month);

			for(int i = 0 ; i < p_m.size();i++){
				JSONObject jo_p_m = new JSONObject();
				switch (i) {
				case 0:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 1:
					jo_p_m.put("name", "���");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 2:
					jo_p_m.put("name", "�ӱ�");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 3:
					jo_p_m.put("name", "ɽ��");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 4:
					jo_p_m.put("name", "���ɹ�");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 5:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 6:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 7:
					jo_p_m.put("name", "������");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 8:
					jo_p_m.put("name", "�Ϻ�");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 9:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 10:
					jo_p_m.put("name", "�㽭");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 11:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 12:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 13:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 14:
					jo_p_m.put("name", "ɽ��");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 15:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 16:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 17:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 18:
					jo_p_m.put("name", "�㶫");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 19:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 20:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 21:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 22:
					jo_p_m.put("name", "�Ĵ�");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 23:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 24:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 25:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 26:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 27:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 28:
					jo_p_m.put("name", "�ຣ");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 29:
					jo_p_m.put("name", "����");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				case 30:
					jo_p_m.put("name", "�½�");
					jo_p_m.put("value", p_m.get(i));
					ja_p_m.add(jo_p_m);
					break;
				default:
					jo_p_m.put("beijing", 0);
					break;
				}

			}





			jo.put("1Hour_china", ja_p_m);


			//�Ǽ�  ������������������������������������������������������������������������������������������������




			JSONArray ja_xj = new JSONArray();

			TreeMap<Integer,String> map_xj= new TreeMap<Integer,String>(new Comparator<Integer>(){     
				public int compare(Integer a,Integer b){return b-a;}});  


			List<String> xj = jedis.hmget("MKTStreaming_p_i_m_100000003",
					"811_"+month,"812_"+month,"813_"+month,"814_"+month,"815_"+month,"821_"+month,"822_"+month,"823_"+month,
					"831_"+month,"832_"+month,"833_"+month,"834_"+month,"835_"+month,"836_"+month,"837_"+month,"841_"+month,
					"842_"+month,"843_"+month,"844_"+month,"845_"+month,"846_"+month,"850_"+month,"851_"+month,"852_"+month,
					"853_"+month,"854_"+month,"861_"+month,"862_"+month,"863_"+month,"864_"+month,"865_"+month);

			for (int i = 0; i < xj.size(); i++)
			{
				switch (i) {
				case 0:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 1:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "���");
					break;
				case 2:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�ӱ�");
					break;
				case 3:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "ɽ��");
					break;
				case 4:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "���ɹ�");
					break;
				case 5:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 6:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 7:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "������");
					break;
				case 8:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�Ϻ�");
					break;
				case 9:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 10:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�㽭");
					break;
				case 11:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 12:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 13:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 14:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "ɽ��");
					break;
				case 15:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 16:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 17:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 18:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�㶫");
					break;
				case 19:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 20:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 21:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 22:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�Ĵ�");
					break;
				case 23:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 24:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 25:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 26:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 27:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 28:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�ຣ");
					break;
				case 29:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "����");
					break;
				case 30:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "�½�");
					break;
				default:
					map_xj.put(Integer.valueOf((String)xj.get(i)), "beijing");
				}

			}

			List list_k_xj = new ArrayList();
			List list_v_xj = new ArrayList();
			Iterator it = map_xj.keySet().iterator();
			while (it.hasNext()) {
				int key = ((Integer)it.next()).intValue();
				String value = (String)map_xj.get(Integer.valueOf(key));
				list_k_xj.add(Integer.valueOf(key));
				list_v_xj.add(value);
			}

			for (int i = 0; i < 5; i++) {
				JSONObject jo_xj = new JSONObject();
				jo_xj.put("name", list_v_xj.get(i));
				jo_xj.put("value", list_k_xj.get(i));
				ja_xj.add(jo_xj);
			}

			jo.put("xj", ja_xj);



			// ��Ȩ  ����������������������������������������������������������������������������������������������������������������������������������������������������������������������������






			JSONArray ja_xq = new JSONArray();

			TreeMap<Integer,String> map_xq= new TreeMap<Integer,String>(new Comparator<Integer>(){     
				public int compare(Integer a,Integer b){return b-a;}});  


			List<String> xq = jedis.hmget("MKTStreaming_p_i_m_102000001",
					"811_"+month,"812_"+month,"813_"+month,"814_"+month,"815_"+month,"821_"+month,"822_"+month,"823_"+month,
					"831_"+month,"832_"+month,"833_"+month,"834_"+month,"835_"+month,"836_"+month,"837_"+month,"841_"+month,
					"842_"+month,"843_"+month,"844_"+month,"845_"+month,"846_"+month,"850_"+month,"851_"+month,"852_"+month,
					"853_"+month,"854_"+month,"861_"+month,"862_"+month,"863_"+month,"864_"+month,"865_"+month);

			for (int i = 0; i < xq.size(); i++)
			{
				switch (i) {
				case 0:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 1:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "���");
					break;
				case 2:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�ӱ�");
					break;
				case 3:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "ɽ��");
					break;
				case 4:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "���ɹ�");
					break;
				case 5:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 6:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 7:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "������");
					break;
				case 8:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�Ϻ�");
					break;
				case 9:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 10:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�㽭");
					break;
				case 11:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 12:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 13:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 14:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "ɽ��");
					break;
				case 15:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 16:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 17:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 18:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�㶫");
					break;
				case 19:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 20:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 21:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 22:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�Ĵ�");
					break;
				case 23:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 24:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 25:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 26:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 27:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 28:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�ຣ");
					break;
				case 29:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "����");
					break;
				case 30:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "�½�");
					break;
				default:
					map_xq.put(Integer.valueOf((String)xq.get(i)), "beijing");
				}

			}

			List list_k_xq = new ArrayList();
			List list_v_xq = new ArrayList();
			Iterator it_xq = map_xq.keySet().iterator();
			while (it_xq.hasNext()) {
				int key = ((Integer)it_xq.next()).intValue();
				String value = (String)map_xq.get(Integer.valueOf(key));
				list_k_xq.add(Integer.valueOf(key));
				list_v_xq.add(value);
			}

			for (int i = 0; i < 5; i++) {
				JSONObject jo_xq = new JSONObject();
				jo_xq.put("name", list_v_xq.get(i));
				jo_xq.put("value", list_k_xq.get(i));
				ja_xq.add(jo_xq);
			}

			jo.put("xq", ja_xq);



			// �����ơ�����������������������������������������������������������������������������������������������������������������������������������������������������������������������-	      





			JSONArray ja_tyy = new JSONArray();

			TreeMap<Integer,String> map_tyy= new TreeMap<Integer,String>(new Comparator<Integer>(){     
				public int compare(Integer a,Integer b){return b-a;}});  


			List<String> tyy = jedis.hmget("MKTStreaming_p_i_m_103000001",
					"811_"+month,"812_"+month,"813_"+month,"814_"+month,"815_"+month,"821_"+month,"822_"+month,"823_"+month,
					"831_"+month,"832_"+month,"833_"+month,"834_"+month,"835_"+month,"836_"+month,"837_"+month,"841_"+month,
					"842_"+month,"843_"+month,"844_"+month,"845_"+month,"846_"+month,"850_"+month,"851_"+month,"852_"+month,
					"853_"+month,"854_"+month,"861_"+month,"862_"+month,"863_"+month,"864_"+month,"865_"+month);

			for (int i = 0; i < tyy.size(); i++)
			{
				switch (i) {
				case 0:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 1:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "���");
					break;
				case 2:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�ӱ�");
					break;
				case 3:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "ɽ��");
					break;
				case 4:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "���ɹ�");
					break;
				case 5:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 6:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 7:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "������");
					break;
				case 8:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�Ϻ�");
					break;
				case 9:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 10:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�㽭");
					break;
				case 11:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 12:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 13:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 14:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "ɽ��");
					break;
				case 15:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 16:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 17:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 18:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�㶫");
					break;
				case 19:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 20:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 21:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 22:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�Ĵ�");
					break;
				case 23:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 24:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 25:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 26:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 27:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 28:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�ຣ");
					break;
				case 29:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "����");
					break;
				case 30:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "�½�");
					break;
				default:
					map_tyy.put(Integer.valueOf((String)tyy.get(i)), "beijing");
				}

			}

			List list_k_tyy = new ArrayList();
			List list_v_tyy = new ArrayList();
			Iterator it_tyy = map_tyy.keySet().iterator();
			while (it_tyy.hasNext()) {
				int key = ((Integer)it_tyy.next()).intValue();
				String value = (String)map_tyy.get(Integer.valueOf(key));
				list_k_tyy.add(Integer.valueOf(key));
				list_v_tyy.add(value);
			}

			for (int i = 0; i < 5; i++) {
				JSONObject jo_tyy = new JSONObject();
				jo_tyy.put("name", list_v_tyy.get(i));
				jo_tyy.put("value", list_k_tyy.get(i));
				ja_tyy.add(jo_tyy);
			}

			jo.put("tyyjy", ja_tyy);

			return jo.toString();

		}catch(Exception e){
			throw e;
		}finally {
			redisUtils.returnResource(jedis);
		}

	}


}
