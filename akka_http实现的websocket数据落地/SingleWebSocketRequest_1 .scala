package akka_http
import akka.actor.ActorSystem
import akka.{ Done, NotUsed }
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import scala.concurrent.Future
import scala.concurrent.Promise

object SingleWebSocketRequest_1 {
  def main(args: Array[String]) = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val printSink: Sink[Message, Future[Done]] =
      Sink.foreach {
        case message: TextMessage.Strict =>
          println(message.text)    //在这里实现数据落地
      }

    val helloSource: Source[Message, NotUsed] =
      Source.single(TextMessage("hello world!"))

    val flow: Flow[Message, Message, Promise[Option[Message]]] =
    Flow.fromSinkAndSourceMat(
    printSink,
    Source.maybe[Message])(Keep.right)

    val (upgradeResponse, promise) =
      Http().singleWebSocketRequest(
      WebSocketRequest("ws://182.92.149.40:30231/akso/subscribe/data/part?appId=dianxin&timestamp=1524196369234&nonce=tttttt&signature=0e061f6e6f0f2403cae3d94984e7cc4a1ea9c2d7"),
      flow)

    val connected = upgradeResponse.map { upgrade =>

      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

    connected.onComplete(println)

  }
}