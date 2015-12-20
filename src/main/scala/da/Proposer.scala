package da

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, UntypedActor, ActorLogging }

import akka.actor.ReceiveTimeout
import java.util.UUID

import scala.concurrent.duration._
import scala.language.postfixOps 

/**
 * Proposer class helper. Contains messages a proposer can receive and
 * actor factory.
 */
object Proposer {
  
  sealed trait ProposerState
  case object InitState extends ProposerState
  case class WaitingLeaderDecision(replies: Set[Int]) extends ProposerState
  case class Leader(nextSeq: Long) extends ProposerState
  case class Listener(heartFailure: ActorRef) extends ProposerState
  
  sealed trait ValueState
  case object NotAssigned extends ValueState
  case object Assigned extends ValueState  
  
  sealed trait SeqState
  /**
   * Means this sequence value was assigned this message and proposed.
   */
  case class Proposed(uuid: UUID) extends SeqState 
  /**
   * Note that I must have used someting else if we had more than 3 acceptors. 
   * Set (v_rnd, value_id, value, original proposed UUID by current proposer)
   */
  case class AcceptedOnce(accId: Int, dataReceived: (Long, UUID, String, UUID)) extends SeqState 
  case class Learned(uuid: UUID, value: String) extends SeqState 
  
  /****** Liveness stuff, heartbeats, parameters, failure detector...etc ******/
  case object LocalBeat
  case class HeartBeat(round: Long)
  case class IncomingHeartBeat(round: Long)
  case object StopBeats
  val heartBeatInterval = 250 milliseconds
  val skippedHeartbeatsCount = 3
  
  /****** Paxos protocol messages ******/
  case class InputValue(id: UUID, value: String)
  case class Phase1B(acceptor_id: Int, rnd: Long)
  case class Phase2B(acceptor_id: Int, rnd: Long, seq: Long, v_rnd: Long, v_id: UUID, v_val: String)
  case class SyncRequest(seq: Long)
  
  val nAcceptors = 3
  val seqStart = 0
  // TODO ensure below is correct
  /**
   * Since we do not know the number of replicas  which is needed to 
   * generate unique id's, assume a large enough one.
   */
  val proposerReplicas = 10
  
  def props(id:Int, commManager: ActorRef) =
    Props( new Proposer(id, proposerReplicas, commManager))
}

class Proposer(id: Int, nReplicas: Int, commManager: ActorRef) 
extends Participant(id, commManager) 
with ActorLogging {
  
  import context.system
  import CommunicationManager._
  import Proposer._
  import Acceptor._
  import Learner._
  
  commManager ! Init
 
 
  override def receive =  PartialFunction[Any, Unit]{
    case CommunicationManagerReady => println(s"Proposer $id is ready to receive and process requests.")
    context.become( paxosImpl(InitState, Map(), Map(), id) )
    beginHeartBeats() 
  }
  
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
         * From the response to this request, I will either know about the leader
         * and become a listener, or I will become the current leader and process this message.
         */
        case InitState =>
          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          log.info(s"c_rnd state for ${self.path.name}  OLD: $c_rnd  NEW: $nextLargestRound")
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(Set()),
              msgState + (uuid -> (msgBody, NotAssigned ) ),
              seqState,
              nextLargestRound ))
              
        case WaitingLeaderDecision(_) =>
          log.info("Received new input while waiting for leader decision.")
           context.become(
            paxosImpl(
              roleState,
              msgState + (uuid -> (msgBody, NotAssigned) ), 
              seqState,
              c_rnd))
              
        case Leader(seq) => 
          log.info(s"Received new input as a leader from $sender")
          msgState.get(uuid) match {
            case Some((_,_)) => () // duplicate
            case None => context.become(
            paxosImpl(
              Leader(seq + 1),
              msgState + (uuid -> (msgBody, Assigned) ), 
              seqState + (seq -> Proposed(uuid) ),
              c_rnd))
          log.info(s" Bound seq to msg and now sending $seq  -> $msgBody")   
              commManager ! Phase2A(c_rnd, seq, uuid, msgBody)
          }      
        case Listener(_) => 
          log.info("Received new input as a listener. Ignoring...")     
      }
      
    case Phase1B(acceptor_id, rnd) =>
      roleState match {
        case WaitingLeaderDecision(replyIds) =>
          log.info(s" Got a decision: $acceptor_id $rnd")
          val newIdSet = replyIds + acceptor_id
          val replyCount = newIdSet.size
          // First check needed to be sure I am the destination of the msg
          if (rnd == c_rnd && replyCount < nAcceptors) { 
            if(replyCount < nAcceptors - 1) { // still need replies 3 acceptor specific
              context.become(
                  paxosImpl(
                      WaitingLeaderDecision(newIdSet),
                      msgState,
                      seqState,
                      c_rnd ))
            }
            else { // we got enough acks
             /*
              * The implementation here executes the first phase only once.
              * thus unlike the original algorithm, we do not care about 
              * the choice of the values, there is no value at the 
              * acceptors anyway. Now is a good time to send the 
              * cached messages from clients  
              */
              println(s"Got enough acks, becoming the leader with ballot $c_rnd...")
              context.become(
                paxosImpl(
                  Leader(seqStart), 
                  msgState,
                  seqState,
                  c_rnd))
             // resend all non-processed messages.
             msgState.foreach( x => if(x._2._2 == NotAssigned) self ! InputValue(x._1, x._2._1))              
            }
          }
        case a => log.debug("Received late Leader election message")
      }
      
    case Phase2B(acc_id, rnd, seq, v_rnd, v_id, v_val) =>
      roleState match {
        case Leader(currentSeq) =>
          if(rnd == c_rnd) {// if this message is actually to me
            // check if I reached the number of needed acks for the seq
            val seqInfo = seqState(seq)
            seqInfo match { 
              case Proposed(original_uuid) => // was acked once before    
                context.become(
                paxosImpl(
                  Leader(currentSeq),
                  msgState,
                  seqState + (seq -> AcceptedOnce(acc_id, (v_rnd, v_id, v_val, original_uuid))),
                  c_rnd))
              case AcceptedOnce(prevId, (pre_vrnd, pre_vid, pre_v_val, original_uuid)) => // acked two times
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
                      Leader(currentSeq), // Must not increment seq here.
                      msgState + (v_id -> (v_val, Assigned)), 
                      seqState + (seq -> Learned(v_id, v_val)),
                      c_rnd))

                    log.debug(s"Proposer value itself accepted. Sending learn from propsoer: $seq $v_val");
                    commManager ! Learn(seq, v_id, v_val) // Must send to proposers and learners.
                  } 
                  else { // otherwise select stored value with largest ballot returned from acceptors.
                    log.debug("Proposed value not accepted but an older one read") // must not happen if no leader failure
                    val (selected_id, selected_val) = 
                      if (pre_vrnd > v_rnd) (pre_vid, pre_v_val) else (v_id, v_val)
                    val original_val = msgState(original_uuid)._1
                    
                    context.become(paxosImpl(
                      Leader(currentSeq), // Again must not increment seq here!
                      (msgState + 
                          // unbind and try again to bin to another seq
                          (selected_id -> (selected_val, Assigned))) + (original_uuid -> (original_val, NotAssigned)), 
                      seqState + (seq -> Learned(selected_id, selected_val)),
                      c_rnd))

                    log.info(s"Retrying to assign message $original_uuid $original_val")
                    self ! InputValue(original_uuid, original_val)
                    log.debug(s"A previously stored value accepted. Sending learn from propsoer: $seq $selected_val");
                    commManager ! Learn(seq, selected_id, selected_val)
                  }
                }
              case _ => () // We don't care otherwise
            }
          }         
        case other => log.debug("received 2B message when not leader " + other)            
      }
      
    case LocalBeat =>
      log.debug("Received own beat")
      roleState match {
        
        case Leader(_) => commManager ! HeartBeat(c_rnd) // Only the leader broadcasts heartbeats
        
        case WaitingLeaderDecision(_) =>
          log.info("Received own beat while waiting decision, will resend request.")
          commManager ! Phase1A(c_rnd)
          
        case InitState => 
          println("Received own beat while at initial state, will try to become leader.")
          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          println(s"Ballot is $nextLargestRound")
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(Set()),
              msgState,
              seqState,
              nextLargestRound))
        case _ => ()
        
      }
         
    case hi@ IncomingHeartBeat(round) =>
      log.info(s"Received heartbeat. Role is $roleState")
      // as long as there is some heart beating among proposers, we are fine.
      roleState match {
        // Refresh the failure detector
        case Listener(heartFailureDetector) => heartFailureDetector ! hi
        case _ => ()
      }
      if (round > c_rnd) {
        println(s"Detected real leader, becoming listener whatever I was doing and stopping beats. I was $roleState")
        // TODO if everything works remove these
//        roleState match {
//          case Leader(seq) =>
//            context.become(paxosImpl(
//                
//                Listener(beginFailureDetector), msgState, seqState, round))
//          case WaitingLeaderDecision(_) => 
//            context.become(paxosImpl(
//                Listener(beginFailureDetector), msgState, seqState, round)) 
//          case _ =>
        // remember the round that beat me so I can generate something higher.
            context.become(paxosImpl
              (Listener(beginFailureDetector), msgState, seqState, round)) 
//        }
      }

      /*
       * I only care when I am a listener, if I am waiting for decision 
       * this will be detected at heartbeat.
       */
    case ReceiveTimeout =>
      roleState match {
        case Listener(failureDetector) =>
          println("No more heartbeats detectable, start another leader election round ")
//          context.stop(failureDetector)
          val nextLargestRound = nextLargestSeq(id, nReplicas, c_rnd)
          log.info(s"Rounds state for ${self.path.name}  OLD: $c_rnd NEW: $nextLargestRound")
          commManager ! Phase1A(nextLargestRound)
          context.become(
            paxosImpl(
              WaitingLeaderDecision(Set()),
              msgState,
              seqState,
              nextLargestRound))
        case _ => ()
      }

    // TODO remove these prints when you finish debugging
    case SyncRequest(seq) =>
      //      println("received request")
      roleState match {
        case Leader(_) =>
          seqState.get(seq) match {
            case Some(x) =>
              x match {
                // This causes multicast which can cause too many messages, but whatever
                // since we cannot use anything else.
                case Learned(uuid, v) =>
                  commManager ! Learn(seq, uuid, v); println("sending val..")
                case _ => ()
              }
            case _ => ()
          }
        case _ => ()
      }

    case other => log.debug(s"Message not processed $other") 
  }
  
  def beginHeartBeats(): ActorRef =  {
    import context.dispatcher
    class ProposerHeart extends Actor {
      log.debug(s"Heartbeats started with parent ${context.parent}")
      val beat = system.scheduler.schedule(
          0 seconds , heartBeatInterval, context.parent, LocalBeat)
          
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
      log.info(s"Failure detector started with parent ${context.parent}")
      context.setReceiveTimeout(heartBeatInterval * skippedHeartbeatsCount )
      
      override def receive = {
        case timeout: ReceiveTimeout =>
          // TODO maybe I can return these
          // stop timeout
//          context.setReceiveTimeout(Duration.Undefined)
          context.parent ! timeout
          
        case IncomingHeartBeat(_) => log.debug("Some leader is alive")
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
