package redis_service;

import java.util.LinkedHashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class redisUtils {
	 // jedis池
    public static JedisPool pool;

    // 静态代码初始化池配置
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        

        //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；  
        config.setTestOnBorrow(false);  
        
        
        config.setMaxIdle(5);   //原来是 -1
        config.setMaxTotal(50);
        config.setMaxWaitMillis(5);
        try {
            pool = new JedisPool(config, "10.140.16.233", 6379);
//            pool = new JedisPool(config, "192.168.200.233", 6379);
        } catch (Exception e) {
            throw new RuntimeException("redis 连接池初始化失败！");
        }
    }

	


	public static void returnResource(Jedis redis) {  
        if (redis != null) {  
            redis.close();
        }  
    }  
    
    
	

	
	
//	public static JedisCluster getRedisCluster(){
//	
//	    JedisPoolConfig poolConfig = new JedisPoolConfig();  
//	    // 最大连接数  
//	    poolConfig.setMaxTotal(3);  
//	    // 最大空闲数  
//	    poolConfig.setMaxIdle(3);  
//	    // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：  
//	    // Could not get a resource from the pool  
//	    poolConfig.setMaxWaitMillis(2000);  
//	    Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();  
//	    nodes.add(new HostAndPort("10.140.16.164", 6379));  
//	    nodes.add(new HostAndPort("10.140.16.165", 6380));  
//	    nodes.add(new HostAndPort("10.140.16.166", 6381));  
//	    nodes.add(new HostAndPort("10.140.16.167", 6382));  
//	    nodes.add(new HostAndPort("10.140.16.168", 6383));  
//	    nodes.add(new HostAndPort("10.140.16.169", 6384));  
//	    nodes.add(new HostAndPort("10.140.16.170", 6385)); 
//	    nodes.add(new HostAndPort("10.140.16.171", 6386)); 
//	    nodes.add(new HostAndPort("10.140.16.172", 6387)); 
//	    nodes.add(new HostAndPort("10.140.16.173", 6387)); 
//	    
//
//	    
//	  
//	    JedisCluster cluster = new JedisCluster(nodes, poolConfig);  
////	    String name = cluster.get("name");  
////	    System.out.println(name);  
////	    cluster.set("age", "18");  
////	    System.out.println(cluster.get("age"));  
////	    try {  
////	        cluster.close();  
////	    } catch (Exception e) {  
////	        e.printStackTrace();  
////	    }
//		return cluster;  
//	
//	
//	}
	
	
}
