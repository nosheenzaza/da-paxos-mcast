package da

import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }

import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress
import java.net.StandardSocketOptions

/**
 * Udp multicast channel and group configuration. Note that
 * I multicast over ipv4.
 */
object UDPMulticastConf {

  final case class InetProtocolFamily() extends DatagramChannelCreator {
    override def create() = DatagramChannel.open(StandardProtocolFamily.INET)
  }

  /**
   * This may need to be executed to make multicast work.
   * sudo route add -net 239.0.0.1 default
   */
  final case class MulticastGroup(group: InetAddress, port:Int, ifaceName: String) extends SocketOptionV2 {
    override def afterBind(s: DatagramSocket) {    
      // TODO I don't think the line below is needed, but let's keep it anyway
      s.getChannel.setOption[Integer](StandardSocketOptions.IP_MULTICAST_TTL, 127)
      val ifaceOption = Option(NetworkInterface.getByName(ifaceName))
      ifaceOption match {
        case Some(iface) => 
          val key = s.getChannel.join(group, iface)
//          println(" Joined multicast group with membership key: $key")
        case None => 
          println("ERROR: Could not find the network interface to join a multicast group!")
          System.exit(0)
      }
    }
  }
}