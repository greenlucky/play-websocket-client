import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

/**
  * Created by lam.nm on 5/29/2017.
  */
object WebSocketClientFlow extends App {

  val config = ConfigFactory.load()

  implicit val system = ActorSystem("lam-system", config)
  implicit val materializer = ActorMaterializer();

  import system.dispatcher

  // Future[Done] is the materialized value of Sink.foreach,
  // emitted when the stream completes
  val incoming: Sink[Message, Future[Done]] =
  Sink.foreach[Message] {
    case message: TextMessage.Strict =>
      println(message.text)
    case message: BinaryMessage.Strict =>
      println(message.data.toString())
  }

  val size: Int = 100000

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.debug("materializer initialInputBufferSize {}", materializer.settings.initialInputBufferSize)
  logger.debug("materializer maxInputBufferSize {}", materializer.settings.maxInputBufferSize)
  logger.debug("materializer maxFixedBufferSize {}", materializer.settings.maxFixedBufferSize)
  logger.debug("materializer outputBurstLimit {}", materializer.settings.outputBurstLimit)
  logger.debug("materializer {}", materializer.settings)

  val shit = Array.fill[Byte](size)(1)
  // send this as a message over the WebSocket
  val outgoing = Source.single(BinaryMessage(ByteString(shit)))

  // flow to use (note: not re-usable!)
  val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest("ws://localhost:9000/api"))

  // the materialized value is a tuple with
  // upgradeResponse is a Future[WebSocketUpgradeResponse] that
  // completes or fails when the connection succeeds or fails
  // and closed is a Future[Done] with the stream completion from the incoming sink
  val (upgradeResponse, closed) =
  outgoing
    .viaMat(webSocketFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
    .toMat(incoming)(Keep.both) // also keep the Future[Done]
    .run()

  // just like a regular http request we can access response status which is available via upgrade.response.status
  // status code 101 (Switching Protocols) indicates that server support WebSockets
  val connected = upgradeResponse.flatMap { upgrade =>
    if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
      Future.successful(Done)
    } else {
      throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
    }
  }

  // in a real application you would not side effect here
  connected.onComplete(println)
  closed.foreach(_ => println("closed"))
}
