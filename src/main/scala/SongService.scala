package codecraft.song

import akka.actor._
import codecraft.song._
import codecraft.platform._
import codecraft.platform.amqp._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

case class SongService() extends ISongStore{
  var songs = Map.empty[String, SongRecord]

  def uuid = java.util.UUID.randomUUID.toString

  def add(cmd: AddSong): AddSongReply = {
    val id = uuid
    songs += (id -> SongRecord(
      id,
      cmd.md5,
      cmd.sha1,
      cmd.name,
      cmd.artist,
      cmd.album,
      cmd.byteLength,
      cmd.url
    ))

    AddSongReply(id)
  }

  def get(cmd: GetSong): GetSongReply = {
    songs get (cmd.id) map { song =>
      GetSongReply(Some(song), None)
    } getOrElse {
      GetSongReply(None, Some("Song does not exist"))
    }
  }

  def onError(exn: Throwable) {
    println(s"$exn")
  }
}

object Main {

  val routingInfo = RoutingInfo(
    SongRoutingGroup.cmdInfo.map {
      case registry => (registry.key, registry)
    } toMap,
    Map(
      SongRoutingGroup.groupRouting.queueName -> SongRoutingGroup.groupRouting
    )
  )

  def main(argv: Array[String]) {
    val system = ActorSystem("service")
    val service = SongService()

    val cloud = AmqpCloud(
      system,
      List(
        "amqp://192.168.99.101:5672"
      ),
      routingInfo
    )

    import system.dispatcher

    cloud.subscribeCmd(
      "cmd.song",
      service,
      5 seconds
    ) onComplete {
      case Failure(e) => throw e
      case Success(_) =>
        println("Subscribed")
        cloud.requestCmd(
          "song.get",
          GetSong("id", "service"),
          5 seconds
        ) onComplete {
          case Failure(e) => throw e
          case Success(res) =>
            println(s"res = $res")
        }
    }
  }
}
