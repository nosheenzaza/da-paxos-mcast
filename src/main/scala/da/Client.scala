package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.actor.ReceiveTimeout

import java.util.UUID

import scala.concurrent.duration._
import scala.language.postfixOps

// TODO fix the empty line exception issue. 

object Client {  
  private case object SendNext
  
  def props(id:Int, commManager: ActorRef, inputs: List[String]) =
    Props( new Client(id, commManager, inputs))
}
/**
 * A client receives an input values list from the main application 
 * sends it one by one to the proposer. Upon learning a sent value, 
 * another values is sent.
 * 
 */
class Client(id:Int, commManager: ActorRef, inputs: List[String]) 
  extends Participant(id, commManager) 
  with ActorLogging {
  
  import context.system
  import Proposer._
  import Client._
  import Learner._
  import CommunicationManager._
  
  commManager ! Init
  
  context.setReceiveTimeout( 1 seconds)
  
  val maxRetry = 30

  override def receive = {
    case CommunicationManagerReady => 
      println(s"Client $id is ready and will start sending input values.")
      val msgID = UUID.randomUUID()
      val now = System.nanoTime
      context.become(messageSender(now, Map(), inputs, Set()))
      self ! SendNext
  }
  
  /**
   * If a message is sent 'maxRetry' times and not delivered, we skip it. 
   */
  def messageSender(
      startTime: Long, 
      sent: Map[UUID, (InputValue, Int)], 
      toSend: List[String], learntSet: Set[Long]): Receive = {
    
    case SendNext => 
      val msgID = UUID.randomUUID()
      val msg = InputValue(msgID, toSend.head)
      commManager ! msg
      context.become(
          messageSender(startTime, sent +  ( msgID -> (msg,0)), toSend.tail,learntSet ) )
          
    // It is very important to send a new input once something new is learned
    // Otherwise sequence numbers will be skipped.
    case Learn(seq, v_id, v_val) =>
      log.info(s" Learned $seq $v_val")
      if (!learntSet.contains(seq)) {
        // delete all messages resent 30 time too
        val retriedLessMaxMinThis = sent.filter(x => x._2._2 < maxRetry) - (v_id)
        context.become(
            messageSender(startTime, retriedLessMaxMinThis, toSend, learntSet + seq))

        val msgWithID = sent.get(v_id)
        msgWithID match {
          case Some((value, _)) => // can be none because msg was confirming a value from another client
            if (toSend.size == 0 && retriedLessMaxMinThis.size == 0) {
              val miliseconds = (System.nanoTime - startTime) / 1000000
              val seconds = miliseconds / 1000
              println(s"All vals decided in $miliseconds milliseconds ($seconds seconds) ");
              context.stop(self)
            } else {
              self ! SendNext
            }
          case None => Unit
        }
      }
     
    case timeout: ReceiveTimeout =>
      log.info("Timeout at client")
        val retryVal = sent.find(_._2._2 < maxRetry)
        retryVal match {
          case Some((v_id, (msg, retries))) =>
            sent + (v_id -> (msg, retries + 1))
            commManager ! msg
          case None => () 
        }
      
    // TODO I think this will block from receiving as well, fix!
    case UdpSenderDied => println("Udp Sender Died, Reviving..."); Thread.sleep(200)    
  }
}
