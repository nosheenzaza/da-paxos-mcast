package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, ActorLogging }

/**
 * Parent of all Paxos roles.
 */
class Participant(id: Int, commManager:ActorRef) extends Actor with ActorLogging{
  log.info("Actor was constructed")
  import context.system
  def receive = {
    case anything =>  println(s"Recieved at particpant implementation $anything")
  }
}
