package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, actorRef2Scala}
import kvstore.Arbiter.*
import akka.pattern.{ask, pipe}

import scala.concurrent.duration.*
import akka.util.Timeout

import scala.collection.immutable

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

  var seqCounter: Long = 0L

  var replicatedCounter: Map[Long, Int] = Map.empty

  var snapshotsInProgress: Map[Snapshot, ActorRef] = Map.empty
  var persistedSnapshots: Set[Snapshot] = Set.empty

  var operationsInProgress: Map[Operation, (ActorRef, Int)] = Map.empty
  var persistedOperations: Set[Operation] = Set.empty

  val persistence = context.system.actorOf(persistenceProps)

  def receive =
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)

  def modify(op: Operation): Unit = {
    val persist = op match {
      case Insert(key, value, id) => Persist(key, Option(value), id)
      case Remove(key, id) => Persist(key, None, id)
    }

    def updateKV(): Unit = {
      op match
        case Insert(key, value, _) => kv = kv + (key -> value)
        case Remove(key, _) => kv = kv - key
    }

    if (!persistedOperations.contains(op)) {
      operationsInProgress.get(op) match {
        case Some((_sender, numOfTry)) if numOfTry < 10 => //todo use 1 sec instead
          operationsInProgress += (op, (_sender, numOfTry + 1))
          persistence ! persist
          context.system.scheduler.scheduleOnce(100.millis, self, op)
        case Some((_sender, _)) =>
          operationsInProgress = operationsInProgress.removed(op)
          _sender ! OperationFailed(op.id)
        case None =>
          if (!operationsInProgress.keySet.contains(op)) {
            operationsInProgress += (op, (sender(), 0))
            updateKV()
          }
          persistence ! persist
          val valueOption = op match
            case Insert(_, value, _) => Option(value)
            case _ => None
          replicators.foreach {
            _ ! Replicate(
              key = op.key,
              valueOption = valueOption,
              id = op.id)
          }
          context.system.scheduler.scheduleOnce(100.millis, self, op)
      }
    }
  }

  /* TODO Behavior for  the leader role. */
  //todo add replication
  val leader: Receive = {
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender() ! GetResult(key, valueOption, id)
    case insert: Insert =>
      modify(insert)
    case remove: Remove =>
      modify(remove)
    case p@Persisted(_, id) =>
      if replicatedCounter.getOrElse(id, 0) >= replicators.size then
        val persistedOp = operationsInProgress.find(_._1.id == id)
        persistedOp match
          case Some((op, (_sender, _))) =>
            operationsInProgress -= op
            persistedOperations += op
            _sender ! OperationAck(id)
          case None =>
            println(s"couldn't find operation for persisted $p")
      else
        //todo what if not replicated on some node for some reason? need to send OperationFailed in that case
        println(s"not replicated yet: $p, $replicatedCounter, ${replicators.size}")
        context.system.scheduler.scheduleOnce(100.millis, self, p)
    case Replicas(replicas) =>
      val _secondaries = replicas.filter(_ != self)

      val secondariesToRemove = secondaries.keySet.diff(_secondaries)
      val secondariesToAdd = _secondaries.diff(secondaries.keySet)

      secondaries.foreach { case (sec, replicator) =>
        if (secondariesToRemove.contains(sec)) {
          replicator ! PoisonPill
          sec ! PoisonPill
        }
      }

      val toAdd = secondariesToAdd.map { r => (r, context.system.actorOf(Replicator.props(r))) }.toMap
      secondaries = secondaries.removedAll(secondariesToRemove) ++ toAdd
      replicators = secondaries.values.toSet

      kv.foreach { case (k, v) =>
        toAdd.values.foreach {
          _ ! Replicate(
            key = k,
            valueOption = Option(v),
            id = seqCounter)
        }
      }
    case Replicated(_, id) =>
      replicatedCounter += (id, replicatedCounter.getOrElse(id, 0) + 1)
  }

  /* TODO Behavior for the replica role. */
  //todo update seq
  val replica: Receive = {
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender() ! GetResult(key, valueOption, id)
    case snapshot@Snapshot(key, valueOption, seq) =>
      if seq < seqCounter then
        sender() ! SnapshotAck(key, seq)
      else if seq > seqCounter then
        ()
      else if !persistedSnapshots.contains(snapshot) then
        if (!snapshotsInProgress.contains(snapshot)) {
          snapshotsInProgress += (snapshot, sender())
          valueOption match
            case Some(value) =>
              kv = kv + (key -> value)
            case None =>
              kv = kv - key
        }
        persistence ! Persist(key, valueOption, seq)
        context.system.scheduler.scheduleOnce(100.millis) { self ! snapshot }
    case Persisted(_, id) =>
      val persistedSnapshot = snapshotsInProgress.find(_._1.seq == id)
      persistedSnapshot match
        case Some((snapshot@Snapshot(key, _, seq), _sender)) =>
          snapshotsInProgress -= snapshot
          persistedSnapshots += snapshot
          seqCounter = seq + 1
          _sender ! SnapshotAck(key, seq)
        case None =>
  }

  arbiter ! Join
