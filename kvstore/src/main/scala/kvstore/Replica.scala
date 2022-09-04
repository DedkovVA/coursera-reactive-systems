package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import kvstore.Arbiter.*
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration.*
import akka.util.Timeout

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var counter: Long = 0L

  var snapshotsInProgress: Set[(Snapshot, ActorRef)] = Set.empty
  var persistedSnapshots: Set[Snapshot] = Set.empty

  val persistence = context.system.actorOf(persistenceProps)

  def receive =
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)

  /* TODO Behavior for  the leader role. */
  //todo add replication
  val leader: Receive =
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender() ! GetResult(key, valueOption, id)
    case Insert(key, valueOption, id) =>
      kv = kv + (key -> valueOption)
      sender() ! OperationAck(id)
    case Remove(key, id) =>
      kv = kv - key
      sender() ! OperationAck(id)

  /* TODO Behavior for the replica role. */
  //todo update seq
  val replica: Receive = {
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender() ! GetResult(key, valueOption, id)
    case snapshot@Snapshot(key, valueOption, seq) =>
      if seq < counter then
        sender() ! SnapshotAck(key, seq)
      else if seq > counter then
        ()
      else
        if (!persistedSnapshots.contains(snapshot)) {
          val pair = (snapshot, sender())
          if (!snapshotsInProgress.contains(pair)) {
            snapshotsInProgress += pair
            valueOption match
              case Some(value) =>
                kv = kv + (key -> value)
              case None =>
                kv = kv - key
          }
          persistence ! Persist(key, valueOption, seq)
          context.system.scheduler.scheduleOnce(100.millis) { self ! snapshot }
        }
    case Persisted(_, id) =>
      val snapshotToPersist = snapshotsInProgress.find(_._1.seq == id)
      snapshotToPersist match
        case Some((snapshot@Snapshot(key, _, seq), _sender)) =>
          snapshotsInProgress -= (snapshot, _sender)
          persistedSnapshots += snapshot
          counter = seq + 1
          _sender ! SnapshotAck(key, seq)
        case None =>
  }

  arbiter ! Join
