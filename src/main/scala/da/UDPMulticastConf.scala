package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString

import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress
import java.net.StandardSocketOptions

import scala.collection.JavaConversions.enumerationAsScalaIterator
import scala.collection.JavaConversions.iterableAsScalaIterable


object UDPMulticastConf {

  final case class InetProtocolFamily() extends DatagramChannelCreator {
    override def create() =
      DatagramChannel.open(StandardProtocolFamily.INET)
  }

  final case class MulticastGroup(group: InetAddress, port:Int) extends SocketOptionV2 {
    override def afterBind(s: DatagramSocket) {
      // TODO just add the interface from a parameter and add a nullness check!!
      // TODO I need to add the following to my routing table so that 
      // multicast works regardless of the current ip address! 
      // sudo route add -net 239.0.0.1 default
      
      //TODO be sure this logic works well on linux too
      // TODO I think ideally no need to have separate address routes in the table, thus
      // I need to search only for the prefix of the address
      // TODO sometimes things will not work unless we remove the routes then insert them again. 
      // maybe I need to make the script do that.
      val  cmd = List ("/bin/sh", "-c", "netstat -nr | grep " + group.getHostAddress + """| tr -s " " "\012" | tail -n1""")
      
      val p = Runtime.getRuntime().exec(cmd.toArray);
      p.waitFor()
      val ifaceName = scala.io.Source.fromInputStream(p.getInputStream).mkString.trim
      
      println("Multicasting on interface " + ifaceName)
      
      // TODO to set the interface correctly, I can cheat and use the java multicastSocket, then
      // call getInterface on it! However I think I cn 
      
      // TODO I don't think the line below is needed, but let's keep it for now
      s.getChannel.setOption[Integer](StandardSocketOptions.IP_MULTICAST_TTL, 127)
      
      val iface = NetworkInterface.getByName(ifaceName)
      val key = s.getChannel.join(group, iface)
      println("Membership key: " + key)
    }
  }
}