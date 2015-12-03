package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }

import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString

import java.util.UUID
import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import UDPMulticastConf._

object Proposer {
  
  case class InputValue(id: UUID, value: String)
  
  def props(id:Int, commManager: ActorRef) =
    Props( new Proposer(id, commManager))
}

// TODO will the grader ensure that, say proposers, acceptors and learners run before clients and that their UDP 
// channel processors are ready?
// or do we have to take care of that? I think we need to take care of that
// easiest way to handle that is to add a please wait message or something... then of course the 
// clients have to retry if messages are not received or if I get the dead messages thing.
// Each value to be agreed upon will  have a UUID, which will also be used to identify the paxos instance.
// there is a map (acts like my log) at each role in the system, the map is (UUID -> (message, info)),
// this holds all information concerning a message and the role uses it to decide what to do with a message it 
// receives concerning it. 

/*
 * I am not sure how to made the reliable broadcast fit in here, I believe I can use the info field of the map
 * as an indicator of the message being r-delivered, a-delivered or what. I think the diff between my 
 * algorithm and that of the atomic broadcast will be that I agree on sequence numbers, not sequence of messages,
 * which should be the same basically. 
 * 
 * The other problem I have now is which roles are important to have up, as non faulty processes? now thinking about it, 
 * I think the replies I get from acceptors will serve as broadcast success indicators, for learners however, I think I strictly 
 * must make every learner, even those who joined later, aware of what has happened so far. I get form the TA that 
 * a learner must ask a proposer for what happened so far, or for a message with a certain sequence number. 
 * 
 * I also doubt the supervision aspect is useful here, as my actors are distributed over different applications.
 * 
 * Now time to think how to fix a proposer, I think a proposer with a certain ID will be able to 
 * be the fixed one until it fails, we inform the system at startup that such a proposer is the coordinator,
 * when it fails, another coordinator must take its place, how I do not know exactly without a round of paxos or 
 * leader elections. I think a phase 1 round of paxos should be enough. when a proposer gets a value for leader other than itself, 
 * it does not proposer anything anymore, does not process consensus requests, and instead receives a heartbeat from the current proposer, when it is abest, the first
 * paxos round is executed again. However, it must keep track of decisions made so far so it can take over when the proposer dies. 
 * thus, the proposer reliably broadcasts entries at the state map whenever it modifies it to the "sleeping" proposers. 
 * 
 * As we can see, each proposer must have a possible state of: coordinator, asleep, or leader voting. When the system starts
 * all proposers do not know the  leader, and will elect one upon the arrival of the first consensus request. they will know
 * about it from the replies of the coordinators. if it is themselves, they take coordinator role, if not, they listen and when they do not
 * receive anything, they execute another round of consensus, and so on... 
 * 
 * Do not forget that the learners contact the proposer for what to print, when we have a fixed coordinator, it should always have the latest view
 * of the state of the messages. The clients should also keep retrying to send the messages unless they get a confirmation that it was decided upon.
 * The coordinator leader is the process that assigns a sequence number to messages it gets   
 */ 

class Proposer(id: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  import Proposer._
  
  commManager ! Init
 
  // TODO this andThen is not working
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => log.info("System is ready to process inputs from Clients")
    context.become( paxosImpl(id) )
  } andThen super.receive
  
  def paxosImpl(
      c_rnd: Long = id, 
      c_val: InputValue = null, 
      rnd: Long = 0, 
      v_rnd: Long = 0,  
      v_val:InputValue = null): 
      Receive = {
    case InputValue(uuid, msgBody) =>
      println("Recieved " + uuid + " " + msgBody + " at " + self )
    case _ => Unit
  }

}
