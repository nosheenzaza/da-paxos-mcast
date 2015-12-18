package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString
import akka.actor.ReceiveTimeout


import java.util.UUID
import scala.concurrent.duration._
import scala.language.postfixOps

import UDPMulticastConf._


object Acceptor {
  case class Phase1A(c_rnd: Long)
  case class Phase2A(c_rnd: Long, seq: Long, id:UUID, v_val: String) // we do not care about message body at all. we propose to agree on storing the seq-UUID pair.
  
  def props(id: Int, commManager: ActorRef) = Props( new Acceptor(id, commManager))
}

class Acceptor(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  
  import Proposer._
  import Acceptor._
  
  commManager ! Init
  
  context.setReceiveTimeout( 2 seconds)
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => println("Acceptor " + id + " is ready receive and process requests.")
    context.become( paxosImpl(Map(), 0) )
    case timeout: ReceiveTimeout => 
      println("Trying again to prepare communication manager")
      commManager ! Init
  } orElse super.receive
  
  def paxosImpl(
      state: Map[Long, (Long, UUID, String)], // seq -> (v_rnd, v_id, v_val)  // v_val for tie break, only take "newest" value
      rnd: Long) // 
  : Receive = {
    case Phase1A(c_rnd) => 
      log.info("Received phase 1A with round number " +  c_rnd + ". Round was " + rnd)
      if(c_rnd >= rnd) {
        context.become(paxosImpl(state, c_rnd))
        // note how the acceptor replies with the same round number
        // as that sent to promise that it will agree on proposed value(s)
        log.info("Sending promise message to proposer with c_rnd " +  c_rnd )
        commManager ! Phase1B(id, c_rnd)
      }
    case Phase2A(c_rnd, seq, new_vid, new_v_val) =>
      log.info("Processing proposed value " + c_rnd + " " + seq + " " + new_vid)
      // I believe c_rnd must never be greater than rnd, the else part below is to verify
      // not true, I could be a later acceptor not aware of previous leaders, due to message loss, it still has to reply though as usual. HOWEVER,
      // I also believe this means that this replica has to upgrade to the new leader, it must be a leader that sent this message based on previous
      // round, if I get everything correctly. This is missing from single-paxos because we do not care about the future rounds as they do not exist.
      if(c_rnd >= rnd) {
        val storedValue = state.get(seq)
        storedValue match {
          // If a value was stored before, return that and do not store current proposed value.
          // this is a stable storage that does not take overwrites!!
          case Some((v_rnd, v_id, stored_v_val)) =>
            log.info("Value already exists in reliable storage!")
            // I am doing this state transition here based on my assumption that only a recent leader can send Phase2A messages.
            context.become( paxosImpl(state, c_rnd))
            // TODO I am sending back the first parameter so I know the message is to me, is this the right thing?
            commManager ! Phase2B(id, c_rnd, seq, v_rnd, v_id, stored_v_val)
            
          // if this value was never stored before, accept the newly proposed value and store it in state
          case None =>
            context.become( paxosImpl(state + (seq -> (c_rnd, new_vid, new_v_val)), c_rnd))
            commManager ! Phase2B(id, c_rnd, seq, c_rnd, new_vid, new_v_val)        
        }
      }     
  }
}
