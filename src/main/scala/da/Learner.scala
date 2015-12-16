package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString

import java.util.UUID

object Learner {
 case class Learn(seq: Long, v_id: UUID, v_val: String) 
 def props(id: Int, commManager: ActorRef) = Props( new Learner(id, commManager))
}
  
class Learner(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging { 
  import context.system
  import CommunicationManager._
  import Proposer._
  import Learner._
 
  commManager ! Init
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => log.info("Learner is ready to process requests")
    context.become( paxosImpl(seqStart, Map(), Set()) )
    // TODO here ask a leader for the decided values to catch up with what has happened so far.
  } orElse super.receive
  
  
   def paxosImpl(
      nextPrint: Long,
      pending: Map[Long, String], // seq -> (v_id, v_val)  // v_val for tie break, only take "newest" value
      printedMessages: Set[UUID]) // to avoid printing duplicates
  : Receive = {
    case Learn(seq, v_id, v_val) => // first print and check if there is more to be printed 
      if(seq == nextPrint) {
        val associatedValPrinted = printedMessages.contains(v_id); 
        if (!associatedValPrinted) println(v_val) //TODO having doubts here concerning duplicate removal, I also need to skip the sequence maybe?
          
      val waitingPoint = printRest( seq + 1) // here we skip the sequence and consider it printed.
      val restofPending = removePrinted(seq + 1, waitingPoint, pending)
        context.become(paxosImpl (
            waitingPoint,
            restofPending,
            printedMessages + v_id
            ) )
      }
      else {
        // otherwise add to cache or skip if already printed associated value
        context.become(paxosImpl (
            nextPrint,
            pending + ( seq -> v_val),
            printedMessages + v_id
            ) )
      }
        
   def printRest(startSeq: Long): Long = {
    val possibleNext = pending.get(startSeq)
    possibleNext match {
      case Some(value) => if (!printedMessages.contains(v_id)) println(value); printRest(startSeq + 1)
      case None => startSeq
    }
  }
   
  def removePrinted(start: Long, end: Long, pending: Map[Long, String]): Map[Long, String] = {
    if(start == end)
      pending
    else {
      val endsRemoved = (pending - start) - end // TODO if element do not exist may get an error
      removePrinted(start + 1, end -1, endsRemoved )
    }
  }
   
  }
  
 
}
