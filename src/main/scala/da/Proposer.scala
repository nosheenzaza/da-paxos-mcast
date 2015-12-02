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

object Proposer {
  
  case class InputValue(v: String, seqNum: Long, senderId: Int)
  
  def props(id:Int, commManager: ActorRef) =
    Props( new Proposer(id, commManager))
}

// TODO will the grader ensure that, say proposers, acceptors and learners run before clients and that their UDP 
// channel processors are ready?
// or do we have to take care of that? I think we need to take care of that
class Proposer(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  import Proposer._
  
  commManager ! Init
 
  // TODO this andThen is not working
  override def receive =  PartialFunction[Any, Unit]{
    case InputValue(v, seq, id) =>
      println("Recieved " + v + " at " + self )
    case CommunicationManagerReady => log.info("System is ready to process inputs from Clients")
  } andThen super.receive
  
//  def paxosImpl(c_rnd: Long, c_val: InputValue, rnd: Long = 0, v_rnd: Long = 0,  v_val:InputValue = null): Receive = {
//    case InputValue(v, seq, id) =>
//      println("Recieved " + v + " at " + self )
//    case _ => Unit
//  }

}
