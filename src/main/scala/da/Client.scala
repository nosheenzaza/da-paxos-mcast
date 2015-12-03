package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
//import akka.camel.{CamelMessage, Consumer, CamelExtension}
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString

import java.util.UUID
import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import UDPMulticastConf._


object Client {
  trait MessageState
  case object Added extends MessageState // means message was just added 
  case object Delivered extends MessageState
  case object Failed extends MessageState
  case class  Retried(nTimes: Int) extends MessageState
  
  case class ToSend(v: String, rest: List[String])
  
  def props(id:Int, commManager: ActorRef, inputs: List[String]) =
    Props( new Client(id, commManager, inputs))
}
/**
 * Client receives the input value from the main application and forwards it to the proposer.
 * The proposer will propose a sequence number for the received massage, because all
 * processes need to agree on the order of messages to be printed. 
 * 
 * TODO ensure with the TA that we only care that the printed output is the same on all learners or if it should also 
 * match the sequence issued at each client as well.  
 * 
 * I am assigning the sequence number at the proposer because it is more representative, I do not care about the order as 
 * issued by one or multiple clients. 
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
   * If all proposers are dead (no response from comm. manager in n seconds), we retry m times, if
   * there is still no response, what can we do? maybe keep stuck of broadcast a poison pill to the system? but
   * again this requires agreement! I think for now I will just keep trying.
   * TODO implement the previous later
   *
   * TODO implement a resend mechanism later on. I need a future for each message, the good thing is that it will
   * be non-blocking!
   */
  override def receive = PartialFunction[Any, Unit] {
    case CommunicationManagerReady => 
      log.info("Client comm. manager ready, becoming a sender..")
      val msgID = UUID.randomUUID()
      context.become(messageSender(Map[UUID, (InputValue, MessageState)]()))
      self ! ToSend(inputs.head, inputs.tail)
  }
  
  def messageSender(messages: Map[UUID, (InputValue, MessageState)]): Receive = {
    case ToSend(input, Nil) => 
      val msgID = UUID.randomUUID()
      val msg = InputValue(msgID, input)
      commManager ! msg
      context.become(messageSender( messages +  ( msgID -> (msg, Added) ) ))
      log.info("Sent all inputs, stopping...") 
//      context.stop(self)
    case ToSend(input, rest) => 
      val msgID = UUID.randomUUID()
      val msg = InputValue(msgID, input)
      commManager ! msg
      context.become(messageSender( messages +  ( msgID -> (msg, Added) ) ))
      self ! ToSend(rest.head, rest.tail)
  }
}
