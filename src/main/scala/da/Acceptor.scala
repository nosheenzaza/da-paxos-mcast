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
import java.util.UUID

import UDPMulticastConf._


object Acceptor {
  case class Phase1A(c_rnd: Long)
  
  def props(id: Int, commManager: ActorRef) = Props( new Acceptor(id, commManager))
}

class Acceptor(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  
  import Proposer._
  import Acceptor._
  
  commManager ! Init
   
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => log.info("Acceptor is ready to process requests")
    context.become( paxosImpl(Map(), 0, 0) )
  } orElse super.receive
  
  def paxosImpl(
      state: Map[UUID, String], 
      rnd: Long, 
      v_rnd: Long)
  : Receive = {
    case a1 @ Phase1A(c_rnd) => 
      log.info("Received phase 1A with round number " +  c_rnd + ". Round was " + rnd)
      if(c_rnd >= rnd) {
        context.become(paxosImpl(state, c_rnd, v_rnd))
        // note how the acceptor replies with the same round number
        // as that sent to promise that it will agree on proposed value(s)
        log.info("Sending promise message to proposer with c_rnd " +  c_rnd )
        commManager ! Phase1B(c_rnd, v_rnd)    
      }
  }
}
