package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString

import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import UDPMulticastConf._

class Participant(id: Int, commManager:ActorRef) extends Actor with ActorLogging{
  log.info(" actor was constructed")
  import context.system
  def receive = {
    //TODO this must be the last thing called at subclasses
    case a =>
      println("Recieved " + a.toString() + " at "+ self)
  }
}
