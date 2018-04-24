akka http官方连接地址
https://doc.akka.io/docs/akka-http/current/client-side/websocket-support.html

这里是为了实现websocket传过来的数据落地，进行后续处理。
该程序可以实现连接不断，连续回传的数据不断接受
ps：没有实现自动续连的逻辑。具体可以看官方示例，最后有这部分。

scala版本：
2.11

依赖：

 <dependencies>
  <dependency>
  <groupId>com.typesafe.akka</groupId>
  <artifactId>akka-http_2.11</artifactId>
  <version>10.1.1</version>
</dependency>
<dependency>
  <groupId>com.typesafe.akka</groupId>
  <artifactId>akka-http-testkit_2.11</artifactId>
  <version>10.1.1</version>
  <scope>test</scope>
</dependency>