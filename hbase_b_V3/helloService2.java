package hbase_b_V3;
import java.util.Map;

//import nginx.clojure.NginxClojureRT;
import nginx.clojure.java.ArrayMap;
//import nginx.clojure.java.Constants;
import nginx.clojure.java.NginxJavaRequest;
import nginx.clojure.java.NginxJavaRingHandler;
//import static nginx.clojure.MiniConstants.BODY;
import static nginx.clojure.MiniConstants.QUERY_STRING;


import static nginx.clojure.MiniConstants.*;

public  class helloService2 implements NginxJavaRingHandler {
  @Override
  public Object[] invoke(Map<String, Object> request) {
	  String result_json = null;
  	//获取请求的参数：query_string
  	NginxJavaRequest req = (NginxJavaRequest) request;
  	String query_string=(String)req.get(QUERY_STRING);
  	try {
  		result_json = hbase_api.resolve_json(query_string);
		
	} catch (Exception e) {
		
		e.printStackTrace();
	}
//返回给客户端的值：query_string
  	return new Object[] {NGX_HTTP_OK,ArrayMap.create(CONTENT_TYPE, "text/plain"),result_json};
  }
}
