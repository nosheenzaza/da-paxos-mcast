package da

import akka.actor.{ ActorRef, Props, Actor, ActorLogging, 
                    AllDeadLetters, DeadLetter, Terminated, 
                    SuppressedDeadLetter }
import akka.io.{ IO, Udp }
import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString
import akka.util.Timeout

import java.net.InetAddress
import java.net.InetSocketAddress

import UDPMulticastConf._

/**
 * Communication Manager commands and actor factory.
 */
object CommunicationManager {
  case object Init
  case object CommunicationManagerReady
  /**
   * When the network load is high and in the presence of message loss, the Udp
   * sender object dies. we notify process roles to slow down when this happens.
   */
  case object UdpSenderDied
  
  def props(address: InetAddress,port: Int, 
            groups: Map[String, InetSocketAddress],
            iface: String) = {
    Props( new CommunicationManager(address, port, groups, iface))
  }
}

/**
 * This class handles sending and receiving Udp datagrams among processes. 
 * It receives send requests from the process paxos role, encodes it 
 * and sends it to the Udp listener of another process.
 */
class CommunicationManager( address: InetAddress,
                            port: Int, 
                            groups: Map[String, InetSocketAddress],
                            iface: String)
  extends Actor
  with ActorLogging {
  
  import context.system
  import Proposer._
  import Acceptor._
  import Learner._
  import UdpHeaders._
  import CommunicationManager._
  
  val udpListener = system.actorOf(
      UdpMulticastListener.props(self, address, port, groups, iface), "udp-receiver")
  val manager = IO(Udp)

  override def receive = {
    case Init =>
      log.debug(s"Preparing UDP sender for  ${sender.path.name}. Please wait...")
      context.become(expectUdpReady(sender))
      manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))

      system.eventStream.subscribe(self, classOf[AllDeadLetters])
  }

  def expectUdpReady(participantSender: ActorRef): Receive = {
    case Udp.SimpleSenderReady =>
     log.debug(s"Udp sender ready and now becoming a router for ${participantSender.path.name}")
     context.watch(sender) 
      participantSender match {
        case _ if participantSender.path.name.contains("client")   =>
          log.debug("Becoming a client router "); 
          context.become(clientRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
          
        case _ if participantSender.path.name.contains("proposer") => 
          log.debug("Becoming a proposer router "); 
          context.become(proposerRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
          
        case _ if participantSender.path.name.contains("acceptor") => 
          log.debug("Becoming an acceptor router "); 
          context.become(acceptorRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
          
        case _ if participantSender.path.name.contains("learner") => 
          log.debug("Becoming a learner router "); 
          context.become(learnerRouter(participantSender, sender))
          participantSender ! CommunicationManagerReady
      }
    case other => handleSenderTermination(participantSender, other)
  }
  
  def clientRouter(client: ActorRef, send: ActorRef): Receive = {
    case inputValue @InputValue(uuid, msgBody) =>
      log.debug(s" recieved input value $uuid $msgBody")
      send ! Udp.Send(ByteString( 
                        inputMessage + separator + uuid + separator + msgBody ), 
                      groups("proposer"))                      
    case l  @ Learn(_,_,_) => client ! l
    case other => handleSenderTermination(client, other)

  }
  
  def proposerRouter(proposer: ActorRef, send: ActorRef): Receive = {
    case inputValue @ InputValue(_, _) => proposer ! inputValue
    case a1 @ Phase1A(seq) => send ! Udp.Send(
      ByteString(phase1A + separator + seq), groups("acceptor"))
    case b1 @ Phase1B(id, rnd) => proposer ! b1
    case a2 @ Phase2A(c_rnd, seq, uuid, msgBody) => send ! Udp.Send(
      ByteString(phase2A + separator + c_rnd + separator + seq +
        separator + uuid + separator + msgBody),
      groups("acceptor"))
    case b2 @ Phase2B(acc_id, c_rnd, seq, v_rnd, v_id, stored_v_val) => proposer ! b2
    case s @ SyncRequest(_) => proposer ! s
    case l @ Learn(seq, selected_id, selected_val) =>
      val learnedToByteString = ByteString(learn + separator +
        seq + separator + selected_id + separator + selected_val)
      send ! Udp.Send(learnedToByteString, groups("learner"))
      send ! Udp.Send(learnedToByteString, groups("client"))
    case h @ HeartBeat(round) => send ! Udp.Send(
      ByteString(heartBeat + separator + round), groups("proposer"))
    case hi @ IncomingHeartBeat(round) => proposer ! hi
    case other => handleSenderTermination(proposer, other)   
  } 
  
  def acceptorRouter(acceptor: ActorRef, send: ActorRef): Receive = {
    case a1 @ Phase1A(_) => acceptor ! a1
    case b1 @ Phase1B(id, rnd) =>
      send ! Udp.Send(ByteString (
          phase1B + separator + id + separator + rnd), groups("proposer"))
    case a2 @ Phase2A(c_rnd, seq, uuid, msgBody) => acceptor ! a2
    case Phase2B(acc_id, c_rnd, seq, v_rnd, v_id, stored_v_val) => 
      send ! Udp.Send( ByteString (
        phase2B + separator +
        acc_id + separator +
        c_rnd + separator + seq + separator +
        v_rnd + separator + v_id + separator + stored_v_val), groups("proposer"))
    case other => handleSenderTermination(acceptor, other)
  }

  def learnerRouter(learner: ActorRef, send: ActorRef): Receive = {
    case l @ Learn(_, _, _) => learner ! l
    case s @ SyncRequest(seq)   => send ! Udp.Send(ByteString(
        syncRequest + separator + seq), groups("proposer"))      
    case d @ DeadLetter(message: Any, sender: ActorRef, recipient: ActorRef) =>
//      println("termination detected by deads. The network is shacky...")
        if (recipient.path.toString.contains("IO-UDP-FF")) {
          context.become(waitTogotoSlowRouter(learner))
          context.stop(recipient)
          manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))
        }
    case Terminated(a) =>
        if (a.path.toString.contains("IO-UDP-FF")) {
//          println(s"termination detected of ${a.path}. The network is shacky...")
          context.become(waitTogotoSlowRouter(learner))
          context.stop(a)
          manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))
        }
  }
  
  def waitTogotoSlowRouter(learner: ActorRef): Receive =  {
    case l @ Learn(_, _, _) => learner ! l
    case Udp.SimpleSenderReady =>
     context.watch(sender)
     context.become(slowLearnerRouter(learner, sender, 0))
    case _ => ()
  }
  
  def slowLearnerRouter(learner: ActorRef, send: ActorRef, skippedSync: Int): Receive = {
    case l @ Learn(_, _, _) => learner ! l
    case s @ SyncRequest(seq)   => 
      
      if (skippedSync > 50) {
        context.become(slowLearnerRouter (learner, send, 0) )
//        println("sending........")
        send ! Udp.Send(ByteString(syncRequest + separator + seq), groups("proposer"))
      }
      else {
        context.become(slowLearnerRouter (learner, send, skippedSync + 1) )
      }
    case d @ DeadLetter(message: Any, sender: ActorRef, recipient: ActorRef) =>
//      println("termination detected by deads")
        if (recipient.path.toString.contains("IO-UDP-FF")) {
          context.become(waitTogotoSlowRouter(learner))
          context.stop(recipient)
          manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))
        }
    case Terminated(a) =>
        if (a.path.toString.contains("IO-UDP-FF")) {
//          println("termination detected of "+a.path)
          context.become(waitTogotoSlowRouter(learner))
          context.stop(a)
          manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))
        }
  }

  def handleSenderTermination(participantSender: ActorRef, msg: Any) {
    msg match {
      case Udp.CommandFailed(_) => println("a failure!!")
      case d @ DeadLetter(message: Any, sender: ActorRef, recipient: ActorRef) =>
//        if (a.path.toString.contains("IO-UDP-FF")) {
          //        println("it was the fucking  UDP sender! resend request")
          context.become(expectUdpReady(participantSender))
          context.stop(recipient)
        participantSender ! UdpSenderDied
//        println(s"Dead letter at manager $d")
      case Terminated(a) =>
//        println(s"Termination detected of $a")
        if (a.path.toString.contains("IO-UDP-FF")) {
          //        println("it was the fucking  UDP sender! resend request")
          context.become(expectUdpReady(participantSender))
          context.stop(a)
          manager ! Udp.SimpleSender(List(InetProtocolFamily(), ReuseAddress(true)))
          participantSender ! UdpSenderDied
        }
      case SuppressedDeadLetter(_,_,_) => ()
      case other => ()//println("another problem" + other)
    }
  }
}

/**
 * Udp headers, would have been better to use a proper serializer like normal
 * sane people do.
 */
object UdpHeaders {
  // TODO use a proper serializer instead of this junk.
  val separator = " "
  val inputMessage = "INPUT"
  val phase1A = "1A"
  val phase1B = "1B"
  val phase2A = "2A"
  val phase2B = "2B"
  val learn = "L"
  val heartBeat = "H"
  val incomingHeartBeat = "HI"
  val syncRequest = "S"
}
