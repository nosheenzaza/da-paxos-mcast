package da


import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.util.ByteString

import java.io.File
import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import scopt.OptionParser

import scala.concurrent.duration._

import UDPMulticastConf._

/**
 * Each role is a member of a multicast group, and can receive messages from the group.
 * To send messages though, the client needs to implement simple UDP sending, and have
 * access to addresses of other groups.
 * 
 * TODO think how to fit all message content in a single UDP packet
 * 
 */
object Main {
  case class Config(roleName: String, id: Int, config: File)

  //TODO put each functionality in a separate function
  def main(args: Array[String]) {
    // TODO what is the best way to avoid this garbage logic?
    // I can just return config and throw an exception when I cannot create one.
    val parsedArgs: Config = getConfig(args)
    val groups = parseGroupsConfig(parsedArgs.config)
    val processGroupAddress = groups(parsedArgs.roleName).getAddress
    val processRolePort = groups(parsedArgs.roleName).getPort
    val inputValues = scala.io.Source.fromFile("values").getLines()
    
    val paxosSystem = ActorSystem("paxos-broadcast")
    
    // Client, listener, proposer or learner depending on the argument sent to the process
    // TODO I think this reflection trick is ugly, will change it later
    // TODO be sure tha neverything is initialized correctly before starting the communication
    val communicationManager = paxosSystem.actorOf( CommunicationManager.props(processGroupAddress, processRolePort, groups) )
    val participant = parsedArgs.roleName match {
      case "client" => paxosSystem.actorOf( Client.props(parsedArgs.id, communicationManager, inputValues.toList),
                                            parsedArgs.roleName + "-" + parsedArgs.id)
      case "proposer" => paxosSystem.actorOf( Proposer.props(parsedArgs.id, communicationManager), 
                                              parsedArgs.roleName + "-" + parsedArgs.id)
      case a => throw new RuntimeException("Invalid process role: " + a) 
    }
  }

  // TODO make the file argument optional
  def getConfig(args: Array[String]): Config = {
    // TODO process the problem of an unkown role somewhere
    val parser = new OptionParser[Config]("scopt") {
      arg[String]("<role>") required () valueName ("<role>") action {
        case (s, c)   => c.copy(roleName = s)
      } text (s" role of the process (client, learner, acceptor, proposer)")

      arg[Int]("<id>") required () valueName ("<id>") action {
        case (x, c) => c.copy(id = x)
      } text (s" id of the process")

      arg[File]("<config>") required () valueName ("<file>") action {
        case (x, c) => c.copy(config = x)
      } text (s" path to configuration file")
    }
    // TODO what is the best way to avoid this garbage?
    parser.parse(args, Config("", -1, new File("config"))) match {
      case Some(c) => c 
      case _ => throw new RuntimeException("Could not create config file!")
    }
  }
  
  // TODO validate the map entries against a regular expression
  def parseGroupsConfig(file: File): Map[String, InetSocketAddress] = {
    try {
      val netConfig = scala.io.Source.fromFile(file).getLines()
      val role = (x: String) => x.substring(0, x.indexOf(" ") - 1)
      val address = (x: String) => x.substring(x.indexOf(" ") + 1, x.indexOf(" ", x.indexOf(" ") + 1))
      val port = (x: String) => x.substring(x.lastIndexOf(" ") + 1, x.length)
      netConfig.foldLeft(Map[String, InetSocketAddress]())((c, x) => c + (role(x) -> (new InetSocketAddress(address(x), port(x).toInt))))
    } catch {case a : Throwable =>  throw new RuntimeException("Failed to parse the configuration file!")}
  }
}