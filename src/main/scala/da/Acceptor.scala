package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString
import akka.actor.ReceiveTimeout

import scala.concurrent.duration._
import scala.language.postfixOps

import java.util.UUID

object Acceptor {
  case class Phase1A(c_rnd: Long)
  case class Phase2A(c_rnd: Long, seq: Long, id: UUID, v_val: String)

  def props(id: Int, commManager: ActorRef) = Props(new Acceptor(id, commManager))
}

class Acceptor(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  import UDPMulticastConf._

  import Proposer._
  import Acceptor._

  commManager ! Init

  context.setReceiveTimeout(2 seconds)

  override def receive = PartialFunction[Any, Unit] {
    case CommunicationManagerReady =>
      println(s"Acceptor $id is ready receive and process requests.")
      context.become(paxosImpl(Map(), 0))
  }

  def paxosImpl(
    // seq -> (v_rnd, v_id, v_val)  // v_val for tie break, only take "newest" value
    state: Map[Long, (Long, UUID, String)],
    rnd: Long) // 
    : Receive = {
    case Phase1A(c_rnd) =>
      log.info("Received phase 1A with round number " + c_rnd + ". Round was " + rnd)
      if (c_rnd >= rnd) {
        context.become(paxosImpl(state, c_rnd))
        log.info("Sending promise message to proposer with c_rnd " + c_rnd)
        commManager ! Phase1B(id, c_rnd)
      }
    case Phase2A(c_rnd, seq, new_vid, new_v_val) =>
      log.info("Processing proposed value " + c_rnd + " " + seq + " " + new_vid)
      if (c_rnd >= rnd) {
        val storedValue = state.get(seq)
        storedValue match {
          // If a value was stored before, return that and do not store current proposed value.
          // this is a stable storage that does not take overwrites!!
          case Some((v_rnd, v_id, stored_v_val)) =>
            log.info("Value already exists in reliable storage!")
            context.become(paxosImpl(state, c_rnd))
            commManager ! Phase2B(id, c_rnd, seq, v_rnd, v_id, stored_v_val)
          // if this value was never stored before, accept the newly proposed value and store it in state
          case None =>
            context.become(paxosImpl(state + (seq -> (c_rnd, new_vid, new_v_val)), c_rnd))
            commManager ! Phase2B(id, c_rnd, seq, c_rnd, new_vid, new_v_val)
        }
      }
  }
}
