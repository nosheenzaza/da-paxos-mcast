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
  val separator = " "
  val inputMessage = "INPUT"
  val phase1A = "1A"
  val phase1B = "1B"
  val phase2A = "2A"
  val phase2B = "2B"
  val learn = "L"
  val heartBeat = "H"
  val incomingHeartBeat = "HI"
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

// TODO test with all processes using the same ip address
class CommunicationManager( address: InetAddress,
                            port: Int, 
                            groups: Map[String, InetSocketAddress])
  extends Actor
  with ActorLogging {
  
  import context.system
  import Proposer._
  import Acceptor._
  import Learner._
  import UdpHeaders._
  import CommunicationManager._
  
  implicit val timeout = Timeout(5 seconds)
  
  val udpListener = system.actorOf(UdpMulticastListener.props(self, address, port, groups), "udp-receiver")
  val manager = IO(Udp)  
  
  // TODO I am worried that if I send the request to process myself beforehand, it will be dropped,
  // check how to fix this (if it is broken)
  override def receive = {
    case Init =>
      println("Preparing UDP sender for " + sender.path.name + ". Please wait...")
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
        case _ if participantSender.path.name.contains("acceptor") => 
          log.info("becoming an acceptor router "); 
          context.become(acceptorRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
        case _ if participantSender.path.name.contains("learner") => 
          log.info("becoming a learner router "); 
          context.become(learnerRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
      }     
  }
  
  def clientRouter(client: ActorRef, send: ActorRef): Receive = {
    case inputValue @InputValue(uuid, msgBody) =>
      log.info(" recieved input value " + uuid + " " + msgBody)
      log.info("sending message to Proposer at" + groups("proposer"))
      send ! Udp.Send(ByteString( inputMessage + separator + uuid + separator + msgBody ), groups("proposer"))
  }
  
  def proposerRouter(proposer: ActorRef, send: ActorRef): Receive = {
    case inputValue @InputValue(_,_) => proposer ! inputValue
    case a1 @ Phase1A(seq) => send ! Udp.Send( ByteString (phase1A + separator + seq), groups("acceptor") )
    case b1 @ Phase1B(rnd) => proposer ! b1
    case a2 @ Phase2A(c_rnd, seq, uuid, msgBody) => send ! Udp.Send( ByteString (phase2A + separator + c_rnd + separator + seq + separator + uuid + separator + msgBody), groups("acceptor") )
    case b2 @ Phase2B(c_rnd, seq, v_rnd, v_id, stored_v_val) => proposer ! b2
    case l  @ Learn(seq, selected_id, selected_val) => send ! Udp.Send( ByteString (learn + separator + seq + separator + selected_id + separator + selected_val), groups("learner") )
    case h  @ HeartBeat(round) => send ! Udp.Send(ByteString (heartBeat + separator + round), groups("proposer"))
    case hi @ IncomingHeartBeat(round) => proposer ! hi
  }
  
  def acceptorRouter(acceptor: ActorRef, send: ActorRef): Receive = {
    case a1 @ Phase1A(_) => acceptor ! a1
    case b1 @ Phase1B(rnd) =>
      log.info("Sending phase 1B to proposers from comm. manager")
      send ! Udp.Send(ByteString (phase1B + separator + rnd), groups("proposer"))
    case a2 @ Phase2A(c_rnd, seq, uuid, msgBody) => acceptor ! a2
    case Phase2B(c_rnd, seq, v_rnd, v_id, stored_v_val) => send ! Udp.Send( ByteString (phase2B + separator + 
                                                                                        c_rnd + separator + seq + separator + 
                                                                                        v_rnd + separator + v_id + separator + stored_v_val), groups("proposer"))
  }
  
   def learnerRouter(learner: ActorRef, send: ActorRef): Receive = {
     case l  @ Learn(_,_,_) => learner ! l 
   }
}
