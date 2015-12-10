package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, UntypedActor, ActorLogging }

import akka.io.{ IO, Udp }
import akka.io.Inet.{ SocketOption, DatagramChannelCreator, SocketOptionV2 }
import akka.remote.DeadlineFailureDetector
import akka.util.ByteString
import akka.actor.ReceiveTimeout

import java.util.UUID
import java.nio.channels.DatagramChannel
import java.net.StandardProtocolFamily
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.InetSocketAddress

import scala.concurrent.duration._
import scala.language.postfixOps 

import UDPMulticastConf._

object Proposer {
  /*
   * TODO mention in the README that the range of ids need to be 0 to n - 1 where n is number of proposers
   * The state 'Leader' state is reached after phase 1A, when the current proposer receives enough acks from
   * the acceptors, however, it could be that before executing the next round, 
   * another proposer attempted to collect acks and succeeded, this will not be detected
   * until we attempt to get a consensus on the next input, if then we see that
   * the majority says they agreed to another leader, we enter the listener state, 
   * otherwise if we succeed, we enter the true leader state and broadcast heartbeats to 
   * other leaders, if this process crashes, the other leaders will detect that
   * at some point, and re-execute elections.
   * 
   * How to tell if I am the true leader? I cannot, but I can tell when I am not if
   * 1) I receive a heartbeat with leader agreement round higher than mine, then 
   * I become a listener and stop sending heartbeats, and watch other heartbeats.
   * 2) I receive an event broadcast with a leader agreement round higher than
   * mine, again  I become a listener and stop sending heartbeats
   * 3) I receive as a reply to a consensus request with a round number
   * other than the one I suggested, then again I become a listener.
   * 
   * What is the heartbeats are late? It is not a problem, again more than one process
   * will think it is the leader and this will be resolved soon. The key really is to use
   * reliable broadcast of the messages.
   * 
   * I believe the reliable broadcast is needed for the learners. 
   * 
   * What should I do as a leader? Other than heartbeats, I must also reliably broadcast 
   * all updates to the state, this I am not sure how to do currently, it could be that
   * clients in the first place r-broadcast their inputs, or that each decision 
   * message is r-broadcast by the leader, I need to think which one is correct, and if both,
   * I use the one that requires sending less messages. 
   * 
   * Actually the leader must reliably broadcast all its actions to other leaders and listeners,
   * what it will do is broadcast the messages to the leader group, eventually the non-leaders 
   * will realize this is the case and stop broadcasting, which means there will be a hopefully
   * short period of time where there are too many messages broadcast, but whatever, safety
   * will be guaranteed.
   */
  
  trait ProposerState
  case object InitState extends ProposerState
  case class WaitingLeaderDecision(replyCount: Int, dataSet: Set[Long]) extends ProposerState
  case class Leader(heart: ActorRef) extends ProposerState
  case class Listener(heartFailure: ActorRef) extends ProposerState
  
  trait ValueState
  case object NotProcessed extends ValueState
  case object Proposed extends ValueState
  
  /******************    Liveness Stuff, heartbeats, failure detector...etc    ******************/
  case class HeartBeat(round: Long)
  case class IncomingHeartBeat(round: Long)
  case object StopBeats
  val heartBeatInterval = 3 seconds
  val skippedHeartbeatsCount = 3
//  val heartbeatFailureDetector = new DeadlineFailureDetector(6 seconds, heartBeatInterval)
  
  
  // TODO maybe it is more elegant to put the rule for encoding 
  // and decoding as methods of message case objects
  // and then just invoke them at comm. manager and UDP listener.
  case class InputValue(id: UUID, value: String)
  case class Phase1B(rnd: Long, v_rnd:Long)
  
  def nAcceptors = 3
  def props(id:Int, commManager: ActorRef) =
    Props( new Proposer(id, 2, commManager))
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
 * 
 * TODO should we pass the number of proposers as a parameter? or let proposers get to know each other? or what should we do?
 * Since there is no need for reliable broadcast and we can make the clients resend, we can use timeouts, in that if the client did not get a message
 * that the value it proposed was learnt, it proposes it again, then if a decision was made we never decide on it again. If a client crashes though, 
 * it may or may not be the case that a value it proposed will ever be learned. But this does not matter.
 */
 
class Proposer(id: Int, nReplicas: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  import Proposer._
  import Acceptor._
  
  commManager ! Init
 
  // TODO this andThen is not working
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => log.info("Proposer is ready to process inputs from Clients")
    context.become( paxosImpl(InitState, Map(), id) )
  } andThen super.receive
  
  /*
   * TODO it would be good to add an illegal state exception or something when 
   * a message is received at a state that does not expect it.
   */
  def paxosImpl(
      roleState: ProposerState,
      state: Map[UUID, (String, ValueState)],
      c_rnd: Long) // Largest seen round number
//      c_val: String = null,
//      rnd: Long = 0, 
//      v_rnd: Long = 0,  
//      v_val:String = null)
  : Receive = {
    
    case InputValue(uuid, msgBody) =>
      log.info("Recieved " + uuid + " " + msgBody + " at " + self )
      roleState match {
        /*
         * No leader was elected or I have not seen a leader yet, execute phase 1A
         * from the response to this request, I will either know about the leader
         * and become a listener, or I will become the current leader and process this message.
         * 
         * do not forget to process the message later on!
         */
        case InitState =>
          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          log.info("Rounds state for " + self.path.name + " OLD: " + c_rnd + " NEW: " + nextLargestRound)
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(0, Set()),
              state + (uuid -> (msgBody, NotProcessed ) ),
              nextLargestRound ))
              
        case WaitingLeaderDecision(_,_) =>
          log.info("Received new input while waiting for leader decision. ")
           context.become(
            paxosImpl(
              roleState,
              state + (uuid -> (msgBody, NotProcessed) ), 
              c_rnd))
              
        case Leader(heart) => 
          log.info("Received new input as a leader. ")
          context.become(
            paxosImpl(
              roleState,
              state + (uuid -> (msgBody, Proposed) ), 
              c_rnd))
              //TODO actually propose here to acceptors
              
        case Listener(_) => 
          log.info("Received new input as a listener. Checking it exists and forwarding...")
          
      }
      
    case Phase1B(rnd, v_rnd) =>
      roleState match {
        /*
         * What if we never get acknowledgements because another leader was faster and he got his acks and we never 
         * got ours? This will be detected if we got heartbeats or messages with round number larger than ours. 
         * 
         * So what takes us out of this state is either getting a next input with larger round or a heartbeat with a larger round.
         * The main idea, the person with the highest round always wins, in all cases, and all others shut up and retreat.
         * 
         * The others though, must remember the round number that beat them, so that can reclaim leader position in the future.
         * 
         * Why don't I do this here? It may be the case that the other leader for some reason never got enough acks, and,
         * not sure if my logic should be highest number/fastest or only fastest. I think only highest number is less
         * tricky, as the fastest can lose in the future and this is fine. Also no need to count others acks, care only
         * about yourself.
         * 
         * TODO I forgot to manage message duplication. I will need to track the ID of the acceptor that sent me the message
         * to distinguish duplicates from real majority distinct replies.
         */
        case WaitingLeaderDecision(replyCount, dataSet) =>
          if (rnd == c_rnd && replyCount < nAcceptors) { // means I did not get a message broadcast to another proposer
            // From the above condition, I can be smart and also keep count of other broadcasted Phase1B messages, aimed at other proposers 
            // I can count those, but no this will not work, because it could be that the other proposer did not get them and only I did.
            if(replyCount < nAcceptors - 1) { // still need replies
              context.become(
                  paxosImpl(
                      WaitingLeaderDecision(replyCount + 1, dataSet + (v_rnd)),
                      state,
                      c_rnd ))
            }
            else { // we got enough acks
              // val k = dataSet.foldLeft(-1L)( (c, x) => if (x > c) x else c ) // get largest v_rnd
              /*
              * The implementation here executes the first phase only once.
              * thus unlike the original algorithm, we do not care about 
              * the choice of the values, there is no value at the 
              * acceptors anyway, but maybe now is a good time to send the 
              * stored messages from clients according to the protocol? 
              * 
              * Note that what ensures the identity of leader and agreement is not this 
              * step, but the next one when a value is agreed upon.
              * I need to handle that somehow. Maybe at the step after I will 
              * retreat the current leader.
              */
              println("Got enough acks, becoming the leader...")
              context.become(
                paxosImpl(
                  Leader(beginHeartBeats(c_rnd)),
                  state,
                  c_rnd))
              
            }
          }
        case a => log.info("Received late Leader election message")
      }

    /*
     * TODO what if I get a heartbeat while I am still waiting for decisions? should I also 
     * retreat or still retry?
     * In anyway, I am forgetting to store the largest seen round, which I would need in order
     * to become a leader again, fix that.
     * 
     * The logoc now works like this, no matter what state I am in, once I get heartbeats with round
     * number larger than mine, I must become a listener.
     * 
     * TODO try not to leave unmatched matches anywhere.
     */
    case hi@ IncomingHeartBeat(round) =>
      log.info("Received heartbeat. Role is " + roleState)
      // as long as there is some heart beating among proposers, we are fine
      roleState match {
        case Listener(heartFailureDetector) => heartFailureDetector ! hi
        case _ => Unit
      }
      // Note that this logic will also detect when a leader changes without the participation of the current
      // listener
      if (round > c_rnd) {
        log.info("Detected real leader, becoming listener whatever I was doing and stopping beats. I was " + roleState)
        roleState match {
          case Leader(heart) =>
            heart ! StopBeats
            context.become(paxosImpl(Listener(beginFailureDetector), state, round)) // remember the round that beat me so I can generate something higher.
          case WaitingLeaderDecision(_,_) => 
            context.become(paxosImpl(Listener(beginFailureDetector), state, round)) 
          case _ => context.become(paxosImpl(Listener(beginFailureDetector), state, round)) // I think this is how it should be, but not sure.
        }
      }
      
    // I only care when I am the listener. Otherwise I can ignore I guess
    case ReceiveTimeout =>
      log.info("No more heartbeats detectable, start another leader election round ")
      
      val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          log.info("Rounds state for " + self.path.name + " OLD: " + c_rnd + " NEW: " + nextLargestRound)
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(0, Set()),
              state,
              nextLargestRound ))
      
    case a => log.info("Message not processed " + a) 
  }
  
  def beginHeartBeats(round: Long): ActorRef =  {
    import context.dispatcher
    class ProposerHeart extends Actor {
      log.info("Heartbeats started with parent " + context.parent)
      val beat = system.scheduler.schedule(0 seconds , heartBeatInterval, commManager, HeartBeat(round))
      override def receive = {
        case StopBeats => beat.cancel()
      } 
      override def postStop() = beat.cancel()
    }
    
    val proposerHeart = context.actorOf(Props(new ProposerHeart()))   
    proposerHeart
  }
  
  def beginFailureDetector(): ActorRef = {
    class HeartFailureDetector extends Actor {
      log.info("Failure detector started with parent " + context.parent)
      // TODO fix this hardcoded limit, and figure out a reasonable timeout depending on the testing env
      context.setReceiveTimeout( 6 seconds)
      override def receive = {
        case timeout: ReceiveTimeout =>
          // stop timeout
          context.setReceiveTimeout(Duration.Undefined)
          context.parent ! timeout
          
        case IncomingHeartBeat(_) => log.info("Some leader is alive")
      }
    }
    val heartFailure = context.actorOf(Props(new HeartFailureDetector()))   
    heartFailure
  }
  
  def nextLargestSeq(id: Int, nReplicas: Int, largestSeenSeq: Long): Long = {
    val largestdivByNReplicas = largestSeenSeq - (largestSeenSeq % nReplicas)
    val nextLargest = largestdivByNReplicas + nReplicas
    val nextLargestUniqueForProcess = nextLargest + id
    nextLargestUniqueForProcess
  }
}
