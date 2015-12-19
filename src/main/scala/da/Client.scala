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
  
  private case object SendNext
  
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
  
  val maxRetry = 30

  override def receive = PartialFunction[Any, Unit] {
    case CommunicationManagerReady => 
      println("Client " + id + " is ready and will start sending input values")
      val msgID = UUID.randomUUID()
      val now = System.nanoTime
      context.become(messageSender(now, Map(), inputs))
      self ! SendNext
//    case timeout: ReceiveTimeout => 
//      println("Trying again to prepare communication manager")
//      commManager ! Init     
  }
  
  /**
   * A message that does not exist is learned. A message in sent is sent but not learned.
   * if a message is sent 30 times and not delivered, we skip it.
   * 
   * TODO I doubt I need a map of sent, I think I need only to watch one element with my current
   * resend procedure. 
   */
  def messageSender(startTime: Long, sent: Map[UUID, (InputValue, Int)], toSend: List[String]): Receive = {
    case SendNext => 
      val msgID = UUID.randomUUID()
      val msg = InputValue(msgID, toSend.head)
      commManager ! msg
      context.become(messageSender(startTime, sent +  ( msgID -> (msg,0)), toSend.tail ) )
          
    case Learn(seq, v_id, v_val) =>
      // delete all messages resent 30 time too
      val retriedLessMaxMinThis = sent.filter(x =>  x._2._2 < maxRetry) - (v_id)
      context.become(messageSender(startTime, retriedLessMaxMinThis, toSend)) // TODO be sure this delete will not crash anything.
         
      val msgWithID = sent.get(v_id)
      
      msgWithID match {
        case Some((value, _)) => // can be non because msg was confirming a value from another client
        if (toSend.size == 0 && retriedLessMaxMinThis.size == 0) {
          val miliseconds = (System.nanoTime - startTime) / 1000000
          val seconds = miliseconds / 1000
          println ("all vals decided in " + miliseconds + " milliseconds " + " (" + seconds + " seconds) ");
          val skippedVals = sent.filter(x =>  x._2._2 >= maxRetry)
          println(s"Skipped inputs:")
          if(skippedVals.size == 0) println("None")
          else 
            skippedVals.foreach(println)     
          context.stop(self)   
        }
        else {
          self ! SendNext
        }
        case None => Unit
      }     
        
      // TODO 30 times reply is too much per value
      // TODO increase timeout
      // TODO add a notification that the message was not delivered
    case timeout: ReceiveTimeout =>
      log.info("Timeout at client")
        val retryVal = sent.find(_._2._2 < maxRetry)
        retryVal match {
          case Some((v_id, (msg, retries))) =>
            sent + (v_id -> (msg, retries + 1))
            commManager ! msg
          case None => 
//            println ("Some inputs permanently dropped after trying to resend up to 30 times. Stopping...")
//            context.stop(self)
        }
    case UdpSenderDied => println("death"); Thread.sleep(500)    
  }
}
