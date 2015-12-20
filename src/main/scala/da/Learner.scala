package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.actor.ReceiveTimeout

import scala.concurrent.duration._
import scala.language.postfixOps

import java.util.UUID

object Learner {
 case class Learn(seq: Long, v_id: UUID, v_val: String) 
 def props(id: Int, commManager: ActorRef) = Props( new Learner(id, commManager))
}

// TODO check if additional printed status stuff will affect the evaluation. Remove if needed.
/**
 * Learner prints stored value and sends requests when timeout is passed.
 */
class Learner(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging { 
  import context.system
  import CommunicationManager._
  import Proposer._
  import Learner._
 
  // TODO I think a larger timeout would work too, since we are printing.
  context.setReceiveTimeout(10 milliseconds)
  
  commManager ! Init

  override def receive = {
    case CommunicationManagerReady =>
//      println(s"Learner $id is ready receive and process requests.")
      context.become(paxosImpl(seqStart, Map(), Set()))
      commManager ! SyncRequest(seqStart) 
  }

  def paxosImpl(
    nextPrint: Long,
    pending: Map[Long, String], // seq -> v_val
    printedMessages: Set[UUID]) // to avoid printing duplicates
    : Receive = {
    case Learn(seq, v_id, v_val) =>
      // Print current and  then check if there is more to be printed 
      if (seq == nextPrint) {
        val associatedValPrinted = printedMessages.contains(v_id);
        if (!associatedValPrinted) {println(v_val); Console.flush}

        val waitingPoint = printRest(seq + 1) // here we skip the sequence and consider it printed.
        val restofPending = removePrinted(seq + 1, waitingPoint, pending)
        context.become(paxosImpl(
          waitingPoint,
          restofPending,
          printedMessages + v_id))
      } else {
        // Otherwise add to cache or skip if already printed associated value
        context.become(paxosImpl(
          nextPrint,
          pending + (seq -> v_val),
          printedMessages + v_id))
        //       commManager ! SyncRequest(nextPrint)
      }

      def printRest(startSeq: Long): Long = {
        val possibleNext = pending.get(startSeq)
        possibleNext match {
          case Some(value) =>
            if (!printedMessages.contains(v_id)) {
              println(value)
              Console.flush
            }
            printRest(startSeq + 1)

          case None => startSeq
        }
      }

      def removePrinted(start: Long, end: Long, pending: Map[Long, String]): Map[Long, String] = {
        if (start == end)
          pending
        else {
          val endsRemoved = (pending - start) - end
          removePrinted(start + 1, end - 1, endsRemoved)
        }
      }
      
    // TODO this gets too slow when joining later and
    // requesting catch up, it becomes fast after values are decided.
    // maybe this is not a bad thing though
    case timeout: ReceiveTimeout =>
      commManager ! SyncRequest(nextPrint)

    //    case UdpSenderDied => Thread.sleep(500)

  } 
}
