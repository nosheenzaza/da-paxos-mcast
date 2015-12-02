package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString

import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import UDPMulticastConf._


object Acceptor {
  case object Phase1A
//  case class Propose(Phase1A, c-rnd: Long)
}

class Acceptor(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  
  override def receive =  PartialFunction[Any, Unit]{
    case s:Int =>
      println("Recieved " + s + " at " + self )
  } orElse super.receive
}
