package da

import akka.actor.{ ActorRef, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.io.Inet.SO.ReuseAddress
import akka.util.ByteString

import java.util.UUID
import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import UDPMulticastConf._

/*
 *  TODO I think I can get away with using the same implementation to 
 *  send messages for all processes on any port, let's see....
 *  This class can be the UDP server, it will recieve messages from other actors in a certain format
 *  other classes will not have to send any UDP messages, and it will 
 *  
 *  TODO I think the design will be nicer if this file encapsulates both UDP sending and 
 *  recieving, and if it implements a trait for communication. 
 */

// TODO test cases for network interface, 1) none 2) wrong name 3) correct one
    // FIXME try to automatically connect on a valid interface by default?
    // TODO do we have to care about creating routes and so in the configuration file?
    // TODO I think it is nicer to pass the InetAddress not the string representation of it.

object UdpMulticastListener {
  def props(communicationManager: ActorRef, address: InetAddress,
            port: Int, 
            groups: Map[String, InetSocketAddress]) = {
    Props(new UdpMulticastListener(communicationManager, address, port, groups))
    
  }
}
class UdpMulticastListener(communicationManager: ActorRef, address: InetAddress,
                           port: Int,
                           groups: Map[String, InetSocketAddress])
  extends Actor
  with ActorLogging {
  
  import context.system
  import Proposer._
  import Acceptor._
  import Learner._
  import UdpHeaders._
  
  val group = MulticastGroup(address, port )
  val manager = IO(Udp)
  val opts = List(InetProtocolFamily(), ReuseAddress(true), group)

  println("Preparing UDP Listener. Please wait...")
  manager ! Udp.Bind(self, new InetSocketAddress(port), opts)

  def receive = {
    case Udp.Bound(local) =>
      log.info("UDP listener Bound to: "+ local.getAddress + ":" + local.getPort)
      context.become(ready(sender))
  }
 
  // I could hack a bit and rely of the fast that I will always get a message  to my port
  // that is suitable for my group, and I can just send it to my socket.
  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      val processed = data.utf8String
//      log.info(" recieved data " + processed)
      val (header, body) = { val array = processed.split(separator, 2)
                             ( array(0), array(1) ) }
      
      header match {
        case `inputMessage` => 
          val (uuid, msgBody) = { val array = body.split(separator)
                                         ( UUID.fromString(array(0)), array(1)) }
          log.info(" sending to proposer through comm. manager: " + body )
          communicationManager ! InputValue(uuid, msgBody)
          
        case `phase1A` =>
          log.info(" sending to acceptor through comm. manager: " + body)
          communicationManager ! Phase1A(body.toLong)
          
        case `phase1B` =>
          log.info(" sending to proposer from listener through comm. manager: " + body)
          val (id, rnd) = { val array = body.split(separator, 2)
                             ( array(0).toInt, array(1).toLong ) }
          communicationManager ! Phase1B(id, rnd)
          
        case `phase2A` => //(c_rnd, seq, uuid, msgBody)
          log.info(" sending phase2A to acceptor through comm. manager: " + body)
          val (c_rnd, seq, uuid, msgBody) = {val array = body.split(separator)
                                              (array(0).toLong, array(1).toLong, UUID.fromString(array(2)), array(3))}
          communicationManager ! Phase2A(c_rnd, seq, uuid, msgBody)
          
        case `phase2B` => //(c_rnd, seq, v_rnd, v_id, stored_v_val)
          log.info(" sending phase2B to proposer through comm. manager: " + body)
          val (acc_id, c_rnd, seq, v_rnd, v_id, stored_v_val) = 
            { val array = body.split(separator)
              (array(0).toInt, array(1).toLong, array(2).toLong, array(3).toLong, UUID.fromString(array(4)), array(5))}
          communicationManager ! Phase2B(acc_id, c_rnd, seq, v_rnd, v_id, stored_v_val)
          
        case `learn` => 
//          log.info(" sending learned val to learner through comm. manager: " + body)
          val (seq, selected_id, selected_val) = 
            { val array = body.split(separator)
              (array(0).toLong, UUID.fromString(array(1)), array(2))}          
          communicationManager ! Learn(seq, selected_id, selected_val)
          
        case `heartBeat` =>
          log.info(" sending heartbeat to other proposers ")
          communicationManager ! IncomingHeartBeat(body.toLong)
          
        case `syncRequest` =>
//          log.info(" sending sync request to other proposers ")
          communicationManager ! SyncRequest
          
        case unknown => log.info("Unkonwn header! " + unknown)
      }
       
    case Udp.Unbind  => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
    case _ => println("something was recieved from " + sender)
  }
}
