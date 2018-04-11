package service_logs

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Connection
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import java.util.Calendar
import org.apache.spark.SparkContext
import kafka.common.TopicAndPartition
import breeze.util.partition
import kafka.message.MessageAndMetadata
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.HashPartitioner
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import scala.collection.mutable.Map
import org.json.JSONArray
import org.json.JSONObject
import scala.collection.mutable.LinkedHashMap
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.streaming.Milliseconds


/**
 * 加了unpersist后，当有宕机后，再启动后不会把宕机期间的数再计算上
 * 可是不加unpersist的话，加了persist而不unpersist，内存就会越来越多
 * 所以就既不加persist也不加unpersist
 */



/**
 * spark-submit --class   service_logs.demo_checkpoint_upstateByKey   --master yarn --deploy-mode client   --conf "spark.dynamicAllocation.enabled=false"     --jars  spark-examples-1.6.0-cdh5.7.1-hadoop2.6.0-cdh5.7.1.jar,jedis-2.9.0.jar,commons-pool2-2.0.jar,./json/xom-1.0d9-2002-09-20.jar,./json/slf4j-log4j12-1.6.2.jar,./json/slf4j-api-1.7.21.jar,./json/log4j-1.2.14.jar,./json/json.jar,./json/json-lib-2.0-jdk15.jar,./json/ezmorph-1.0.3.jar,./json/commons-logging-api-2003-04-07.jar,./json/commons-lang-exception-2.0-2003-09-06.jar,./json/commons-collections-3.2.jar   0519_testspark11.jar
 */





  

//targetTypeId=1&modelID=8109990005&pwd=bdms200120170420&channelId=2&prvcID=846&actionID=201501010005&targetObjectId=13398911030&qryUserProRuleId=102000001&user=bdms2001

/**
 * * 限速 *
Direct Approach (NoReceivers) 的接收方式也是可以限制接受数据的量的。你可以通过设置 
spark.streaming.kafka.maxRatePerPartition 来完成对应的配置。需要注意的是，这里是对每个Partition进行限速。
所以你需要事先知道Kafka有多少个分区，才好评估系统的实际吞吐量，从而设置该值。

相应的，spark.streaming.backpressure.enabled 参数在Direct Approach 中也是继续有效的。
 */



object demo_checkpoint_upstateByKey_2 {

 

  def main(args: Array[String]) {  
    val loc = new Locale("en")  
    val fm = new SimpleDateFormat("yyyyMM",loc)
//    val fm_day = new SimpleDateFormat("yyyyMMddHHmm",loc)
    val sparkConf = new SparkConf().setAppName("UpstateByKeyCount")  
    
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
//    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5000")    //这是针对直连方式的限速
//    
//    sparkConf.set("spark.receivers.num", "5")
    sparkConf.set("spark.streaming.receiver.maxRate", "1000")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
     
//    sparkConf.set("spark.streaming.unpersist", "true")
    sparkConf.set("spark.streaming.concurrentJobs", "30")
    
//    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    
    val ssc = new StreamingContext(sparkConf, Milliseconds(4500L))    //Seconds(5)
    
    ssc.checkpoint("hdfs://nsctgmkt/yhl/test/checkpoint77")
//    val topicSet = Set("tpnginxlog") //   tpnginxlog
//    val brokers = "10.140.16.201:9092,10.140.16.221:9092,10.140.16.223:9092"//
//    val kafkaParams = scala.collection.immutable.Map[String, String](
//        "metadata.broker.list" -> brokers,
//        "auto.offset.reset" -> "smallest",
//        "serializer.class" -> "kafka.serializer.StringEncoder")
//      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2) 
        
      val topics = Set(("tpnginxlog", 5)).toMap
      val messages = KafkaUtils.createStream(ssc, "10.140.16.201:2181/mktkafka", "yhl" ,topics).map(_._2) 
      val total = messages.filter { x => x.contains("qryUserProRuleId")}
                          .map { x => 
                             //接口
                             val qryUserProRuleId = x.split("qryUserProRuleId=")(1).split("&")(0)  
                             //省份
                             val prvcID = x.split("prvcID=")(1).split("&")(0) 
                             //月
                             val month = fm.format(System.currentTimeMillis())
                             //自开始以来的数据  key是固定的  Pro
                             val history = "history"//x.split("&qryUser")(1).split("RuleId")(0)
//                             val day = fm_day.format(System.currentTimeMillis())
                             
                             (qryUserProRuleId,prvcID,month,history)
                          }
                       .filter{case(x,y,z,k) =>x.length() > 0 && y.length() > 0 && z.length() > 0 && k.length() > 0}
                      // .persist()
                    
                          
      //所有接口当月
      val m = total.map(x=>(x._3,1L)).reduceByKey(_+_)//.persist()
      //所有接口当天
      val a = total.map(x=>(x._4,1L)).reduceByKey(_+_)//.persist()
      //分接口当月
      val i_m = total.map(x=>(x._1+"_"+x._3,1L)).reduceByKey(_+_)//.persist()
      //分省当月
      val p_m = total.map(x=>(x._2+"_"+x._3,1L)).reduceByKey(_+_)//.persist()
      //分省分接口当月
      val p_i_m = total.map(x=>(x._2+"_"+x._1+"_"+x._3,1L)).reduceByKey(_+_)//.persist()

      m.checkpoint(Seconds(3600))
      m.foreachRDD{rdd => 
//        rdd.checkpoint()
        rdd.foreachPartition{ partition =>
          
          
          
          
           
              object InternalRedisClient extends Serializable{
              private var pool:JedisPool = null
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
                makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,false,false,10000)
              }
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,
                  minIdle:Int,testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long) :Unit={
                if(pool == null){
                  val poolConfig = new GenericObjectPoolConfig()
                  poolConfig.setMaxTotal(maxTotal)
                  poolConfig.setMaxIdle(maxIdle)
                  poolConfig.setMinIdle(minIdle)
                  poolConfig.setTestOnBorrow(testOnBorrow)
                  poolConfig.setTestOnReturn(testOnReturn)
                  poolConfig.setMaxWaitMillis(maxWaitMillis)
                  pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
                  val hook = new Thread{
                    override def run = pool.destroy()
                  }
                  sys.addShutdownHook(hook.run)
                }
              }
              def getPool:JedisPool = {
                assert(pool != null)
                pool
              }
            }



          
          
            //redis config
            val maxTotal = 1000
            val maxIdle = 10
            val minIdle = 1
            val redisHost = "192.168.200.233"
            val redisPort = 6379
            val redisTimeout = 60000
            val dbIndex = 1
            InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
              val poolInst =  InternalRedisClient.getPool
              var jedis = poolInst.getResource()
                  while(partition.hasNext){
                       jedis.select(0)
                       jedis.hincrBy("MKTstreaming_m",fm.format(System.currentTimeMillis()),partition.next()._2)
                  }
            
//              jedis.hincrBy("streaming_m","m",records._2)
//              InternalRedisClient.getPool.returnResource(jedis)
//              poolInst.destroy()
                          
              jedis.quit()
              
           
        }                
      }
                          
      a.checkpoint(Seconds(3600))                 
      a.foreachRDD{rdd => 
//        rdd.checkpoint()
        rdd.foreachPartition{ partition =>
          
          
          
           
              object InternalRedisClient extends Serializable{
              private var pool:JedisPool = null
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
                makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,false,false,10000)
              }
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,
                  minIdle:Int,testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long) :Unit={
                if(pool == null){
                  val poolConfig = new GenericObjectPoolConfig()
                  poolConfig.setMaxTotal(maxTotal)
                  poolConfig.setMaxIdle(maxIdle)
                  poolConfig.setMinIdle(minIdle)
                  poolConfig.setTestOnBorrow(testOnBorrow)
                  poolConfig.setTestOnReturn(testOnReturn)
                  poolConfig.setMaxWaitMillis(maxWaitMillis)
                  pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
                  val hook = new Thread{
                    override def run = pool.destroy()
                  }
                  sys.addShutdownHook(hook.run)
                }
              }
              def getPool:JedisPool = {
                assert(pool != null)
                pool
              }
            }



          
            //redis config
            val maxTotal = 1000
            val maxIdle = 10
            val minIdle = 1
            val redisHost = "192.168.200.233"
            val redisPort = 6379
            val redisTimeout = 60000
            val dbIndex = 1
            InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
            val poolInst =  InternalRedisClient.getPool
            var jedis = poolInst.getResource()
                   while(partition.hasNext){
                             jedis.select(0)
                             jedis.hincrBy("MKTstreaming_a", "a",  //fm_day.format(System.currentTimeMillis())
                                 partition.next()._2)
                   }
//              jedis.close()
//              poolInst.destroy()
                          
              jedis.quit()
        }                
      }
      
      
      i_m.checkpoint(Seconds(3600))
      
      i_m.foreachRDD{rdd => 
//        rdd.checkpoint()
        rdd.foreachPartition{ partition =>
          
          
          
           
              object InternalRedisClient extends Serializable{
              private var pool:JedisPool = null
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
                makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,false,false,10000)
              }
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,
                  minIdle:Int,testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long) :Unit={
                if(pool == null){
                  val poolConfig = new GenericObjectPoolConfig()
                  poolConfig.setMaxTotal(maxTotal)
                  poolConfig.setMaxIdle(maxIdle)
                  poolConfig.setMinIdle(minIdle)
                  poolConfig.setTestOnBorrow(testOnBorrow)
                  poolConfig.setTestOnReturn(testOnReturn)
                  poolConfig.setMaxWaitMillis(maxWaitMillis)
                  pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
                  val hook = new Thread{
                    override def run = pool.destroy()
                  }
                  sys.addShutdownHook(hook.run)
                }
              }
              def getPool:JedisPool = {
                assert(pool != null)
                pool
              }
            }



            //redis config
            val maxTotal = 1000
            val maxIdle = 10
            val minIdle = 1
            val redisHost = "192.168.200.233"
            val redisPort = 6379
            val redisTimeout = 60000
            val dbIndex = 1
            InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
            val poolInst =  InternalRedisClient.getPool
            var jedis = poolInst.getResource()
                  while(partition.hasNext){
                     val t = partition.next()
                     if(t._1 != null || t._1.length() > 0 && t._2 != null || t._2 > 1L){
                         jedis.select(0) 
                         jedis.hincrBy("MKTstreaming_i_m",
                             "streaming_"+t._1.split("_")(0)+"_"+fm.format(System.currentTimeMillis()),   //"_m"
                             t._2)
                     }
                     
                     
                  }  
           
              //              poolInst.destroy()         
              jedis.quit()
        }                
      }
      
      
      p_m.checkpoint(Seconds(3600))
      
      p_m.foreachRDD{rdd => 
//        rdd.checkpoint()
        rdd.foreachPartition{ partition =>
          
          
           
              object InternalRedisClient extends Serializable{
              private var pool:JedisPool = null
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
                makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,false,false,10000)
              }
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,
                  minIdle:Int,testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long) :Unit={
                if(pool == null){
                  val poolConfig = new GenericObjectPoolConfig()
                  poolConfig.setMaxTotal(maxTotal)
                  poolConfig.setMaxIdle(maxIdle)
                  poolConfig.setMinIdle(minIdle)
                  poolConfig.setTestOnBorrow(testOnBorrow)
                  poolConfig.setTestOnReturn(testOnReturn)
                  poolConfig.setMaxWaitMillis(maxWaitMillis)
                  pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
                  val hook = new Thread{
                    override def run = pool.destroy()
                  }
                  sys.addShutdownHook(hook.run)
                }
              }
              def getPool:JedisPool = {
                assert(pool != null)
                pool
              }
            }



            //redis config
            val maxTotal = 1000
            val maxIdle = 10
            val minIdle = 1
            val redisHost = "192.168.200.233"
            val redisPort = 6379
            val redisTimeout = 60000
            val dbIndex = 1
            InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
            val poolInst =  InternalRedisClient.getPool
            var jedis = poolInst.getResource()
                while(partition.hasNext){
                   val t = partition.next()
                     if(t._1 != null || t._1.length() > 0 && t._2 != null || t._2 > 1L){
                  
                   jedis.select(0)
                   jedis.hincrBy("MKTstreaming_p_m",
                       "streaming_"+t._1.split("_")(0)+"_"+fm.format(System.currentTimeMillis()),  //"_m"
                       t._2)
                     }
                }
                 
             //              poolInst.destroy()
                          
              jedis.quit()
          
        }                
      }
      
      p_i_m.checkpoint(Seconds(3600))
      p_i_m.foreachRDD{rdd => 
//        rdd.checkpoint()
        rdd.foreachPartition{ partition =>
      
          
          
           
              object InternalRedisClient extends Serializable{
              private var pool:JedisPool = null
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
                makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,false,false,10000)
              }
              def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,maxTotal:Int,maxIdle:Int,
                  minIdle:Int,testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long) :Unit={
                if(pool == null){
                  val poolConfig = new GenericObjectPoolConfig()
                  poolConfig.setMaxTotal(maxTotal)
                  poolConfig.setMaxIdle(maxIdle)
                  poolConfig.setMinIdle(minIdle)
                  poolConfig.setTestOnBorrow(testOnBorrow)
                  poolConfig.setTestOnReturn(testOnReturn)
                  poolConfig.setMaxWaitMillis(maxWaitMillis)
                  pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
                  val hook = new Thread{
                    override def run = pool.destroy()
                  }
                  sys.addShutdownHook(hook.run)
                }
              }
              def getPool:JedisPool = {
                assert(pool != null)
                pool
              }
            }



            //redis config
            val maxTotal = 1000
            val maxIdle = 10
            val minIdle = 1
            val redisHost = "192.168.200.233"
            val redisPort = 6379
            val redisTimeout = 60000
            val dbIndex = 1
            InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
             val poolInst =  InternalRedisClient.getPool
            var jedis = poolInst.getResource()
                while(partition.hasNext){
                   val t = partition.next()
                     if(t._1 != null || t._1.length() > 0 && t._2 != null || t._2 > 1L){
                   jedis.select(0)//("streaming_"+records._1.split("_")(0)+"_"+records._1.split("_")(1)+"_m", records._2)
                   jedis.hincrBy("MKTStreaming_p_i_m_"+t._1.split("_")(1),
                       t._1.split("_")(0)+"_"+fm.format(System.currentTimeMillis()), 
                       t._2)
                     }
                } 
        
             //              poolInst.destroy()
                          
              jedis.quit()
         
                       
          
        }                
      }

    ssc.start()  
    ssc.awaitTermination()  

}     
  
 
  
  
}



