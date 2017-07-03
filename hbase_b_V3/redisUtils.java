package hbase_b_V3;

import java.util.LinkedHashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class redisUtils {
	 // jedis��
    public static JedisPool pool;

    // ��̬�����ʼ��������
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        

        //��borrowһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�  
        config.setTestOnBorrow(false);  
        config.setTestOnReturn(false);
        config.setMaxTotal(3000);   //3000
        config.setMaxIdle(15);   //ԭ���� -1
        config.setMinIdle(1);
        config.setMaxWaitMillis(5);
        try {
            pool = new JedisPool(config, "10.140.16.233", 6379);
//            pool = new JedisPool(config, "192.168.200.233", 6379);
        } catch (Exception e) {
            throw new RuntimeException("redis ���ӳس�ʼ��ʧ�ܣ�");
        }
    }

	

	public static void returnResource(Jedis redis) {  
        if (redis != null) {  
//        	pool.destroy();
            redis.quit();
        }  
    }  
    
    
	

	
	
//	public static JedisCluster getRedisCluster(){
//	
//	    JedisPoolConfig poolConfig = new JedisPoolConfig();  
//	    // ���������  
//	    poolConfig.setMaxTotal(3);  
//	    // ��������  
//	    poolConfig.setMaxIdle(3);  
//	    // �������ȴ�ʱ�䣬����������ʱ�仹δ��ȡ�����ӣ���ᱨJedisException�쳣��  
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
