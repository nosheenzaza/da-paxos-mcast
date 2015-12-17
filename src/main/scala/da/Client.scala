package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
//import akka.camel.{CamelMessage, Consumer, CamelExtension}
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString
import akka.actor.ReceiveTimeout

import java.util.UUID
import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import scala.concurrent.duration._
import scala.language.postfixOps

import UDPMulticastConf._

// TODO fix the empty line exception issue. 
object Client {
  sealed trait MessageState
  case object Added extends MessageState // means message was just added 
  case object LearnedInput extends MessageState
//  case object Failed extends MessageState
//  case class  Retried(nTimes: Int) extends MessageState
  
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
  import Learner._
  import CommunicationManager._
  
  commManager ! Init
  
  context.setReceiveTimeout( 1 seconds)
  
//  def endpointUri = "stream:in"

  /**
   * If all proposers are dead (no response from comm. manager in n seconds), we retry m times, if
   * there is still no response, what can we do? maybe keep stuck of broadcast a poison pill to the system? but
   * again this requires agreement! I think for now I will just keep trying.
   * TODO implement the previous later
   *
   * TODO implement a resend mechanism later on. I need a future for each message, the good thing is that it will
   * be non-blocking!
   * 
   * TODO it would be good to find a way to make a deterministic choice regarding the order of messages from 
   * clients, maybe this is why it is better to assign a sequence number here not at the proposer actually.
   * This way, if a proposer dies, I would only need to resent form the clients since I do not care
   * about the crashing of a client, as I can just ignore the rest of its messages, or maybe store it on
   * stable storage. The TA mentioned something about killing the clients, so maybe better add that anyway,
   * and only remove the values when I know that the value is learnt.  
   */
  override def receive = PartialFunction[Any, Unit] {
    case CommunicationManagerReady => 
      println("Client " + id + " is ready and will start sending input values")
      val msgID = UUID.randomUUID()
      context.become(messageSender(Map()))
      self ! ToSend(inputs.head, inputs.tail)
  }
  
  def messageSender(state: Map[UUID, (InputValue, MessageState)]): Receive = {
    case ToSend(input, Nil) => 
      val msgID = UUID.randomUUID()
      val msg = InputValue(msgID, input)
      commManager ! msg
      context.become(messageSender( state +  ( msgID -> (msg, Added) ) )) 

    case ToSend(input, rest) => 
      val msgID = UUID.randomUUID()
      val msg = InputValue(msgID, input)
      commManager ! msg
      context.become(messageSender( state +  ( msgID -> (msg, Added) ) ))
      self ! ToSend(rest.head, rest.tail)
      
      case Learn(seq, v_id, v_val) =>
        val msgWithID = state.get(v_id)
        msgWithID match {
          case Some((value,_)) =>// can be non because msg was confirming a value from another client
            context.become(messageSender(
                state + (v_id -> ((value, LearnedInput)))
                ))
          case None => Unit
        }      
      case timeout: ReceiveTimeout =>
        // on timeout, resend everything not yet learned. 
        // TODO is this good enough or do I need a timer that would resend independant of any event?
        state.toList.filter( x => x._2._2 match { 
          case Added =>
            true 
          case _ => false}).foreach(x => commManager ! x._2._1)  
  }
}
