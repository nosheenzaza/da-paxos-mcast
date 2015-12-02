package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
//import akka.camel.{CamelMessage, Consumer, CamelExtension}
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString

import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import UDPMulticastConf._


object Client {
  case class ToSend(v: String, rest: List[String], seq: Long)
  
  def props(id:Int, commManager: ActorRef, inputs: List[String]) =
    Props( new Client(id, commManager, inputs))
}
/**
 * Client receives the input value from the main application and forwards it to the proposer.
 * The proposer will propose a sequence number for the recieved massage, because all
 * processes need to agree on the order of messages to be printed. 
 */
class Client(id:Int, commManager: ActorRef, inputs: List[String]) 
  extends Participant(id, commManager) 
//  with Consumer 
  with ActorLogging {
  
  import context.system
  import Proposer._
  import Client._
  import CommunicationManager._
  
  commManager ! Init
  
  def endpointUri = "stream:in"
   
  /**
   * If all proposers are dead (no response from comm manager in n seconds), we retry m times, if 
   * there is still no response, what can we do? maybe keep stuck of broadcast a poison pill to the system? but
   * again this requires agreement! I think for now I will just keep trying. 
   * TODO implement the previous later 
   */
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => //context.become(processInputFile)
         log.info("Client comm. manager ready, becoming a sender..")
         self ! ToSend(inputs.head, inputs.tail, 0)
    case ToSend(_, Nil, _) => 
      log.info(" sent all inputs, stopping...") 
      context.stop(self)
    case ToSend(input, rest, seq) => 
      commManager ! InputValue(input, seq, id)
      self ! ToSend(rest.head, rest.tail, seq + 1)
  } orElse super.receive
  
}
