package da

import akka.actor.{ ActorRef, Props, Actor, ActorLogging, AllDeadLetters, DeadLetter, Terminated }
import akka.io.{ IO, Udp }

import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString

import java.util.UUID
import java.net.InetAddress
import java.net.InetSocketAddress


/**
 * Listener actor factory.
 */
object UdpMulticastListener {
  def props(communicationManager: ActorRef, address: InetAddress,
            port: Int, 
            groups: Map[String, InetSocketAddress],
            iface: String) = {
    Props(new UdpMulticastListener(communicationManager, address, port, groups, iface))
  }
}

/**
 * Listens to messages multicast to the designated process group
 * and forwards then to the communication manager.
 */
class UdpMulticastListener(communicationManager: ActorRef, address: InetAddress,
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
  import UDPMulticastConf._
  
  val group = MulticastGroup(address, port, iface)
  val manager = IO(Udp)
  val opts = List(InetProtocolFamily(), ReuseAddress(true), group)

  println("Preparing UDP Listener. Please wait...")
  manager ! Udp.Bind(self, new InetSocketAddress(port), opts)

  def receive = {
    case Udp.Bound(local) =>
      log.info(s"UDP listener Bound to: ${local.getAddress} : ${local.getPort}")
      context.become(ready(sender))
    case other => handleLisneterTermination(other)
  }
  
  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      val processed = data.utf8String
      log.debug(" recieved data " + processed)
      val (header, body) =
        { val array = processed.split(separator, 2); (array(0), array(1)) }
      
      header match {
        case `inputMessage` => 
          val (uuid, msgBody) = {
              val array = body.split(separator)
              (UUID.fromString(array(0)), array(1))
            }
          log.debug(s"Sending to proposer through comm. manager: $body")
          communicationManager ! InputValue(uuid, msgBody)
          
        case `phase1A` =>
          log.debug(s"Sending to acceptor through comm. manager: $body")
          communicationManager ! Phase1A(body.toLong)
          
        case `phase1B` =>
          log.debug(s"Sending to proposer from listener through comm. manager: $body")
          val (id, rnd) = {
            val array = body.split(separator, 2)
            (array(0).toInt, array(1).toLong)
          }
          communicationManager ! Phase1B(id, rnd)
          
        case `phase2A` =>
          log.debug(s"Sending phase2A to acceptor through comm. manager: $body")
          val (c_rnd, seq, uuid, msgBody) = {
            val array = body.split(separator)
            (array(0).toLong, array(1).toLong, UUID.fromString(array(2)), array(3))
          }
          communicationManager ! Phase2A(c_rnd, seq, uuid, msgBody)
          
        case `phase2B` => //(c_rnd, seq, v_rnd, v_id, stored_v_val)
          log.debug(s"Sending phase2B to proposer through comm. manager: $body")
          val (acc_id, c_rnd, seq, v_rnd, v_id, stored_v_val) = {
            val array = body.split(separator)
            (array(0).toInt, array(1).toLong, array(2).toLong, 
             array(3).toLong, UUID.fromString(array(4)), array(5))
          }
          communicationManager ! Phase2B(acc_id, c_rnd, seq, v_rnd, v_id, stored_v_val)
          
        case `learn` => 
          log.debug(s"Sending learned val to learner through comm. manager: $body")
          val (seq, selected_id, selected_val) = {
            val array = body.split(separator)
            (array(0).toLong, UUID.fromString(array(1)), array(2))
          }          
          communicationManager ! Learn(seq, selected_id, selected_val)
          
        case `heartBeat` =>
          log.debug("Sending heartbeat to other proposers ")
          communicationManager ! IncomingHeartBeat(body.toLong)
          
        case `syncRequest` =>
          log.debug("Sending sync request to other proposers ")
          communicationManager ! SyncRequest(body.toLong)
          
        case unknown => log.debug("Unkonwn header! " + unknown)
      }
       
    // TODO maybe change this logic later.  
    case Udp.Unbind  => println("Unbind received!! "); socket ! Udp.Unbind
    case Udp.Unbound => println("Unbound!!!"); context.stop(self)
    case other  => handleLisneterTermination(other)
  }
  
  def handleLisneterTermination(msg: Any) {
    msg match {
      case Udp.CommandFailed(command) => println(s"A failure of $command")
      case d @ DeadLetter(message: Any, sender: ActorRef, recipient: ActorRef) =>
        println(s"Dead letter at listener $d")
      case Terminated(a) =>
        println(s"Termination at listener detected of $a")
        if (a.path.toString.contains("IO-UDP-FF")) {
          //        println("it was the fucking  UDP sender! resend request")
          context.become(receive)
          manager ! Udp.Bind(self, new InetSocketAddress(port), opts)
        }
      case other => ()
    }
  }
}
