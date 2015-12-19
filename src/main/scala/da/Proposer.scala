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
   * will think it is the leader and this will be resolved soon.
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
   * 
   *
   *  ----------------------------------  Fault Tolerance  ----------------------------------- 
   *  
   * It is important to think about critical message loss paths. I do not care about message loss 
   * in actors residing at the same virtual machine. because (TODO put a link). Critical paths are
   * between different processes exchanging UDP messages. For those, we guarantee
   * (At Least One delivery) which means there can be duplicates, but the actor will keep attempting to send
   * a message until it is delivered. 
   * 
   * The general philosophy for message loss is: the goal of confirmation is stopping message retries. The confirmer
   * will "know" that the confirmee learnt the ack by the confirmee shutting up, as long as the confirmee does 
   * not shut up, we just keep sending confirmations, if the number of confirmations grows too large, we show a warning
   * that some process may be dead. Note that, it is possible to skip intermediate messages 
   *  
   * Example interactions:
   * 1) send phase1A, if no heartbeat starts, send it again.
   * 2) send phase1B from acceptor, in case we get another request, sent again, no need to track these.
   * 3) propose a value, if no majority reply is obtained within a certain time, propose again. key is is keeping state.
   *  Note how the acceptors do not need to resend messages, they are not the askers. seems like only the asker does.
   * 4) learners, on any receive timeout, will request the current sequence number from proposers. proposer will send that
   * or decide on it, and  send it.
   * 5) Clients, on any timeout, will scan their message list and resend all values not yet decided.
   * 6) Learners too, on any timeout, will request the current seq from proposer, and also any immediately following
   * sequence values in separate messages.
   *
   * 
   *
   *
   * 
   * We can see that the proposer has the most detailed message loss handling, which is natural as it is the server.
   * Note that we do not need to do anything special if a proposer crashes. the other listerenrs will repeat asking for what
   * they need, and the algorithm, along with correct unique ids will handle guaranteeing safety.
   * Note that the only case the proposer is an asker, is when it asks the acceptors for a message. then at its own
   * heartbeat, it will resend the commands not responded to. 
   */
   /*
    * How do we use paxos to order messages?
    * We create a paxos round to agree on the message assigned to a sequence number. That round is identified
    * by that sequence number itself. The questions here are:
    *   1) How do we know that a unique message is assigned and so it is not assigned again to another sequence?
    *   Because the sent decision for the round includes the id of the message. so other than storing 
    *   the decision, we also mark the message as used or something. This should be enough, we need to ensure though
    *   that in case of failure, the change to both is consistent, this can be easily asserted because 
    *   
    * How do we handle assigning duplicates or not assignign messages to rounds?
    * The answer is: WE DON'T CARE (at least not in the paxos protocol)
    * 
    * Only thing paxos cares about and is ensured here is: same values observed at each participant.
    * 
    * Additional thing we (care?) about are:
    *  1) No input remains un-assigned: This we can do best-effort for at clients. , as long
    *  as the client does not learn about a certain value, it keeps retrying to send. From
    *  discussions with leonardo this seems enough.
    *  2) No input is assigned more than once: This can be verified at the learners, before printing
    *  a certain sequence, verify that the bundled input was not printed before. From paxos propoerties,
    *  it is guaranteed that all learners will drop duplicate values in the same manner, and the final
    *  output will be identical.
    *  
    *  The nice thing is that the sequence number being agreed on can be local to the proposer, and paxos guarantees 
    *  that no one sequence is assigned multiple times. The thing is
    *  1) we are agreeing on making a sequence number AND a message.
    *  
    *  So what if a late leader re-proposes different values for already printed sequences? and THESE 
    *  arrive at another late Learner??? I think a quorum of acceptors must remember what rounds
    *  were decided and then by paxos properties the same value will be chosen. This is also why it is safe to 
    *  send the value directly from the acceptors to learners! because of the leader did not receive before
    *  enough acks, it would have never been able to send a value in the first place!!!!
    * 
    */
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
 * I am not sure how to make the reliable broadcast fit in here, I believe I can use the info field of the map
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
  
  sealed trait ProposerState
  case object InitState extends ProposerState
  case class WaitingLeaderDecision(replies: Set[Int]) extends ProposerState
  case class Leader(nextSeq: Long) extends ProposerState
  case class Listener(heartFailure: ActorRef) extends ProposerState
  
  sealed trait ValueState
  case object NotAssigned extends ValueState
//  case object Assigned extends ValueState // the proposal of a seq assigned this value is sent to acceptors
  case object Assigned extends ValueState // this proposal was accepted and now time to send it to learners. 
  
  
  sealed trait SeqState
//  case object NotProcessed extends SeqState // this sequence value never had a round
  case class Proposed(uuid: UUID) extends SeqState // this sequence value was assigned this message and proposed
  // TODO this must be different if there are more than 3 acceptors 
  case class AcceptedOnce(accId: Int, dataReceived: (Long, UUID, String, UUID)) extends SeqState // Set (v_rnd, value_id, value, original proposed UUID by me!)
  case class Learned(uuid: UUID, value: String) extends SeqState // this sequence value was accepted and it being taught to learners
  // Note how UUID is present here too, becauses it is possible that the value assigned differes from that proposed.
  // TODO make sure you remove the assignment if the returned UUID is different! 
  
  
  
  /******************    Liveness Stuff, heartbeats, failure detector...etc    ******************/
  case object LocalBeat
  case class HeartBeat(round: Long)
  case class IncomingHeartBeat(round: Long)
  case object StopBeats
  val heartBeatInterval = 250 milliseconds
  val skippedHeartbeatsCount = 3
//  val heartbeatFailureDetector = new DeadlineFailureDetector(6 seconds, heartBeatInterval)
  
  
  // TODO maybe it is more elegant to put the rule for encoding 
  // and decoding as methods of message case objects
  // and then just invoke them at comm. manager and UDP listener.
  case class InputValue(id: UUID, value: String)
  case class Phase1B(acceptor_id: Int, rnd: Long)
  case class Phase2B(acceptor_id: Int, rnd: Long, seq: Long, v_rnd: Long, v_id: UUID, v_val: String)
  case class SyncRequest(seq: Long)
  
  def nAcceptors = 3
  
  val seqStart = 0
  
  def props(id:Int, commManager: ActorRef) =
    Props( new Proposer(id, 2, commManager))
}


 
class Proposer(id: Int, nReplicas: Int, commManager: ActorRef) extends Participant(id, commManager) with ActorLogging {
  import context.system
  import CommunicationManager._
  import Proposer._
  import Acceptor._
  import Learner._
  
  commManager ! Init
 
  // TODO this andThen is not working
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => println("Proposer " + id + "  is ready receive and process requests.")
    context.become( paxosImpl(InitState, Map(), Map(), id) )
    beginHeartBeats() //TODO this can be dangerous, as I am not stopping previous hearts. Maybe stop them if needed :P
  } andThen super.receive
  
  
  def paxosImpl(
      roleState: ProposerState,
      msgState: Map[UUID, (String, ValueState)],
      seqState: Map[Long, SeqState], // seq ->SeqState
      c_rnd: Long /* Largest seen round number*/): Receive = {
    
    case InputValue(uuid, msgBody) =>
      log.info(s"Recieved $uuid $msgBody at $self" )
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
              WaitingLeaderDecision(Set()),
              msgState + (uuid -> (msgBody, NotAssigned ) ),
              seqState,
              nextLargestRound ))
              
        case WaitingLeaderDecision(_) =>
          log.info("Received new input while waiting for leader decision. ")
           context.become(
            paxosImpl(
              roleState,
              msgState + (uuid -> (msgBody, NotAssigned) ), 
              seqState,
              c_rnd))
              
        case Leader(seq) => 
          log.info("Received new input as a leader from "+ sender)
          context.become(
            paxosImpl(
              Leader(seq + 1),
              msgState + (uuid -> (msgBody, Assigned) ), 
              seqState + (seq -> Proposed(uuid) ),
              c_rnd))
          log.info(" Bound seq to msg and now sending " + seq + " -> " + msgBody )   
              commManager ! Phase2A(c_rnd, seq, uuid, msgBody)
              
        // TODO I can add messages here as well instead of making clients resend
        case Listener(_) => 
          log.info("Received new input as a listener. Checking it exists and forwarding...")     
      }
      
    case Phase1B(acceptor_id, rnd) =>
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
        case WaitingLeaderDecision(replyIds) =>
          log.info("got a decision! " + acceptor_id + " rnd")
          val newIdSet = replyIds + acceptor_id
          val replyCount = newIdSet.size
          if (rnd == c_rnd && replyCount < nAcceptors) { // means I did not get a message broadcast to another proposer
            if(replyCount < nAcceptors - 1) { // still need replies
              context.become(
                  paxosImpl(
                      WaitingLeaderDecision(newIdSet),
                      msgState,
                      seqState,
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
              println(s"Got enough acks, becoming the leader with ballot $c_rnd...")
              context.become(
                paxosImpl(
                  Leader(seqStart), //TODO if you want to synchronize with older leaders, you should not put 0 here but check the seq state.
                  msgState,
                  seqState,
                  c_rnd))
             // resend all non-processed messages.
             msgState.foreach( x => if(x._2._2 == NotAssigned) self ! InputValue(x._1, x._2._1))
              
            }
          }
        case a => log.info("Received late Leader election message")
      }
      
    case Phase2B(acc_id, rnd, seq, v_rnd, v_id, v_val) =>
      roleState match {
        case Leader(currentSeq) =>
          if(rnd == c_rnd) {// if this message is actually to me
            // check if I reached the number of needed messages for the seq
            // TODO will there always be a value here? it should exist I guess as I only consider replies to myself
            val seqInfo = seqState(seq)
            seqInfo match { 
              case Proposed(original_uuid) => 
                
                context.become(
                paxosImpl(
                  Leader(currentSeq),
                  msgState,
                  seqState + (seq -> AcceptedOnce(acc_id, (v_rnd, v_id, v_val, original_uuid))),
                  c_rnd))
              case AcceptedOnce(prevId, (pre_vrnd, pre_vid, pre_v_val, original_uuid)) => // we got two replies which is what we need
                /*
                 * Because of stop failure, we are safe to accept any two proposal and taking the once with
                 * the most recent v_val, because an old acceptor coming back to live and ruining things
                 * cannot happen, we need to be careful though to store the value accepted if it differs 
                 * from what we had. I should not care about higher values, as they are not to me but
                 * the real leader not me and it will take care of things and I will know that later.
                 * 
                 * Any reply with a number less that mine indicates I got a value proposed by an older leader
                 * and I must accept that. Only if both replies match my proposal I can start sending to learners.
                 */
                if (prevId != acc_id) {
                  if (v_rnd == c_rnd && pre_vrnd == c_rnd) { // value I proposed can go! update message and seq states
                    context.become(paxosImpl(
                      Leader(currentSeq), // no need to change seq here.
                      msgState + (v_id -> (v_val, Assigned)), // a marker so we do not use it again, but maybe I can delete it, no i wont so I know it is used if client sends again
                      seqState + (seq -> Learned(v_id, v_val)),
                      c_rnd))

                    log.info("Proposer value itself accepted. Sending learn from propsoer: " + seq + " " + v_val);
                    commManager ! Learn(seq, v_id, v_val) // Must send to proposers and learners.
                  } 
                  else { // otherwise select stored value with largest timestamp returned from acceptors.
                    log.info("proposed value not accepted but an older one") // must not happen if no leader failure
                    // val oldMessageState = msgState(v_id)
                    val (selected_id, selected_val) = if (pre_vrnd > v_rnd) (pre_vid, pre_v_val) else (v_id, v_val)

                    val original_val = msgState(original_uuid)._1
                    
                    context.become(paxosImpl(
                      Leader(currentSeq), // again no need to change seq here!
                      (msgState + (selected_id -> (selected_val, Assigned))) + (original_uuid -> (original_val, NotAssigned)), // unbind and try again to bin to another seq
                      seqState + (seq -> Learned(selected_id, selected_val)),
                      c_rnd))

                    log.info("retrying to assign message " + original_uuid + " " + original_val)
                    self ! InputValue(original_uuid, original_val)
                    log.info("A previously stored value accepted. Sending learn from propsoer: " + seq + " " + selected_val);
                    commManager ! Learn(seq, selected_id, selected_val)
                  }
                }
              case _ => () // we don't care otherwise
            }
          }
          
        case other => log.info("received 2B message when not leader " + other)  
          
      }
      
    case LocalBeat =>
      log.info("Received own beat")
      roleState match {
        case Leader(_) => commManager ! HeartBeat(c_rnd) // only the leader broadcasts heartbeats
        case WaitingLeaderDecision(_) =>
          log.info("Received own beat while waiting decision, will resend request")
//          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
//          println(s"ballots is $nextLargestRound")
          commManager ! Phase1A(c_rnd)
//          context.become(
//            paxosImpl(
//              WaitingLeaderDecision(Set()),
//              msgState,
//              seqState,
//              nextLargestRound))
        case InitState => 
          println("Received own beat while at initial state, will try to become leader")
          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          println(s"ballots is $nextLargestRound")
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(Set()),
              msgState,
              seqState,
              nextLargestRound))
        case _ => Unit
        
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
        println("Detected real leader, becoming listener whatever I was doing and stopping beats. I was " + roleState)
        roleState match {
          // one can be more effecient and hand over the current sequence and sequence state, TODO maybe I do that later if there is time. 
          case Leader(seq) =>
//            heart ! StopBeats
            context.become(paxosImpl(Listener(beginFailureDetector), msgState, seqState, round)) // remember the round that beat me so I can generate something higher.
          case WaitingLeaderDecision(_) => 
            context.become(paxosImpl(Listener(beginFailureDetector), msgState, seqState, round)) 
          case _ => context.become(paxosImpl(Listener(beginFailureDetector), msgState, seqState, round)) // I think this is how it should be, but not sure.
        }
      }

// I only care when I am listener, if I am waiting for decision this will be detected at heartbeat.
    case ReceiveTimeout =>
      roleState match {
        case Listener(failureDetector) =>
          println("No more heartbeats detectable, start another leader election round ")
//          context.stop(failureDetector)
          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          log.info("Rounds state for " + self.path.name + " OLD: " + c_rnd + " NEW: " + nextLargestRound)
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(Set()),
              msgState,
              seqState,
              nextLargestRound))
        case _ => ()
      }

    case SyncRequest(seq) =>
      roleState match {
        case Leader(_) =>
          seqState.get(seq) match {
            case Some(x) =>
              x match {
                // TODO this causes multicast which can cause too many messages, but whatever. 
                case Learned(uuid, v) => commManager ! Learn(seq, uuid, v)
                case _                => ()
              }
            case _ => ()
          }
        case _ => ()
      }
          
          
//          foreach( x => x._2 match {
//            
//            case other => ()// here I can bind something and send to the learner but whatever
//          })
//    case Learn(seq, v_id, v_val) =>

    case a => log.info("Message not processed " + a) 
  }
  
  def beginHeartBeats(): ActorRef =  {
    import context.dispatcher
    class ProposerHeart extends Actor {
      log.info("Heartbeats started with parent " + context.parent)
      val beat = system.scheduler.schedule(0 seconds , heartBeatInterval, context.parent, LocalBeat)
      override def receive = {
        case StopBeats => beat.cancel()
      } 
      override def postStop() = beat.cancel()
    }
    
    val proposerHeart = context.actorOf(Props(new ProposerHeart())) //, self.path.name + "-heart-" + id)   
    proposerHeart
  }
  
  def beginFailureDetector(): ActorRef = {
    class HeartFailureDetector extends Actor {
      log.info("Failure detector started with parent " + context.parent)
      // TODO fix this hardcoded limit, and figure out a reasonable timeout depending on the testing env
      context.setReceiveTimeout(heartBeatInterval * skippedHeartbeatsCount )
      override def receive = {
        case timeout: ReceiveTimeout =>
          // stop timeout
//          context.setReceiveTimeout(Duration.Undefined)
          context.parent ! timeout
          
        case IncomingHeartBeat(_) => log.info("Some leader is alive")
      }
    }
    val heartFailure = context.actorOf(Props(new HeartFailureDetector())) //, self.path.name + "-detector-" + id)   
    heartFailure
  }
  
  def nextLargestSeq(id: Int, nReplicas: Int, largestSeenSeq: Long): Long = {
    val largestdivByNReplicas = largestSeenSeq - (largestSeenSeq % nReplicas)
    val nextLargest = largestdivByNReplicas + nReplicas
    val nextLargestUniqueForProcess = nextLargest + id
    nextLargestUniqueForProcess
  }
}
