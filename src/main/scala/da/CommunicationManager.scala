package da

import akka.actor.{ ActorRef, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString
import akka.util.Timeout
import akka.pattern.ask

import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import scala.concurrent.duration._

import UDPMulticastConf._

/**
 * This class receives requests from Paxos participants, and forward them to the UDP listener in order 
 * to multicast them to other groups, it also receives messages from UDP listener in order to forward them to the current process.
 * 
 * TODO for paxos, it would be clear from the type of the message which role sent it, I think I can clarify from the type name 
 * where it came from otherwise
 * 
 * TODO how can we deal with the fact that UDP datagrams might be dropped? or do we not have to worry about that?
 */
object UdpHeaders {
  def separator = " "
  def inputMessage = "INPUT" + separator
}
/**
 * TODO I think I will separate the sender part, put it with the listener and hide both behind a single manager actor
 */
object CommunicationManager {
  case object Init
  case object CommunicationManagerReady
  
  def props(address: InetAddress,
                            port: Int, 
                            groups: Map[String, InetSocketAddress]) = {
    Props( new CommunicationManager(address, port, groups))
  }
}

class CommunicationManager( address: InetAddress,
                            port: Int, 
                            groups: Map[String, InetSocketAddress])
  extends Actor
  with ActorLogging {
  
  import context.system
  import Proposer._
  import UdpHeaders._
  import CommunicationManager._
  
  implicit val timeout = Timeout(5 seconds)
  
  val udpListener = system.actorOf(UdpMulticastListener.props(self, address, port, groups))
  val manager = IO(Udp)  
  
  // TODO I am worried that if I send the request to process myself beforehand, it will be dropped,
  // check how to fix this (if it is broken)
  override def receive = {
    case Init =>
      context.become(expectUdpReady(sender))
      manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))
    
  }

  def expectUdpReady(participantSender: ActorRef): Receive = {
    case Udp.SimpleSenderReady =>
     log.info("Udp sender ready and now processing becoming router for " + participantSender.path.name)
      participantSender match {
        case _ if participantSender.path.name.contains("client")   =>
          log.info("becoming a client router "); 
          context.become(clientRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
        case _ if participantSender.path.name.contains("proposer") => 
          log.info("becoming a proposer router "); 
          context.become(proposerRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
      }     
  }
  
  def clientRouter(client: ActorRef, send: ActorRef): Receive = {
    case inputValue @InputValue(v, seq, id) =>
      log.info(" recieved input value " + v)
      log.info("sending message to Proposer at" + groups("proposer"))
      send ! Udp.Send(ByteString(inputMessage + v + separator + seq + separator + id), groups("proposer"))
  }
  
  def proposerRouter(proposer: ActorRef, send: ActorRef): Receive = {
    case inputValue @InputValue(_,_,_) => proposer ! inputValue 
  }
}
