package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor }

import java.io.File
import java.net.InetSocketAddress

import scopt.OptionParser

import scala.concurrent.duration._
import scala.language.postfixOps 

/**
 * Driver application. Reads the configuration and command line args,
 * then transforms into the specified Paxos role. 
 */
object Main {
  
  case class Config(roleName: String, id: Int, config: File, iface: String)
  
  def main(args: Array[String]) {
    val parsedArgs: Config = getConfig(args)
    val groups = parseGroupsConfig(parsedArgs.config)
    val processGroupAddress = groups(parsedArgs.roleName).getAddress
    val processRolePort = groups(parsedArgs.roleName).getPort
    val iface = parsedArgs.iface
    val inputValues = scala.io.Source.fromFile("values").getLines()
    
    val paxosSystem = ActorSystem("paxos-broadcast")
    val communicationManager = paxosSystem.actorOf(
        CommunicationManager.props(processGroupAddress, processRolePort, groups, iface), "comm-manager" )
    
    // TODO create case objects for role names and headers
    val participant = parsedArgs.roleName match {
      case "client" => paxosSystem.actorOf( 
          Client.props(parsedArgs.id, communicationManager, inputValues.toList),
                       parsedArgs.roleName + "-" + parsedArgs.id)
      case "proposer" => paxosSystem.actorOf( 
          Proposer.props(parsedArgs.id, communicationManager), 
          parsedArgs.roleName + "-" + parsedArgs.id)
      case "acceptor" => paxosSystem.actorOf(
          Acceptor.props(parsedArgs.id, communicationManager),
          parsedArgs.roleName + "-" + parsedArgs.id)
      case "learner" => paxosSystem.actorOf(
          Learner.props(parsedArgs.id, communicationManager),
          parsedArgs.roleName + "-" + parsedArgs.id)
      case other => throw new RuntimeException("Invalid process role: " + other) 
    }
  }

  def getConfig(args: Array[String]): Config = {
    val parser = new OptionParser[Config]("scopt") {
      arg[String]("<role>") required () valueName ("<role>") action {
        case (s, c)   => c.copy(roleName = s)
      } text (" role of the process (client, learner, acceptor, proposer)")

      arg[Int]("<id>") required () valueName ("<id>") action {
        case (x, c) => c.copy(id = x)
      } text (" id of the process")

      arg[File]("<config>") required () valueName ("<file>") action {
        case (x, c) => c.copy(config = x)
      } text (" path to configuration file")
      
      arg[String]("<iface>") required () valueName ("<iface>") action {
        case (x, c) => c.copy(iface = x)
      }
    }

    parser.parse(args, Config("", -1, new File("config"), "")) match {
      case Some(c) => c 
      case _ => println("Program will exit now..."); System.exit(0); null 
    }
  }
  
  def parseGroupsConfig(file: File): Map[String, InetSocketAddress] = {
    try {
      val netConfig = scala.io.Source.fromFile(file).getLines()
      val role = (x: String) => x.substring(0, x.indexOf(" ") - 1)
      val address = (x: String) => x.substring(x.indexOf(" ") + 1, x.indexOf(" ", x.indexOf(" ") + 1))
      val port = (x: String) => x.substring(x.lastIndexOf(" ") + 1, x.length)
      netConfig.foldLeft(Map[String, InetSocketAddress]())((c, x) => 
        c + (role(x) -> (new InetSocketAddress(address(x), port(x).toInt))))
    } 
    catch {case a : Throwable =>  throw new RuntimeException("Failed to parse the configuration file!")}
  }
}