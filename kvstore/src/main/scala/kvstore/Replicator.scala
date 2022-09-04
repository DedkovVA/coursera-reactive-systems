package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import scala.concurrent.duration.*

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor:
  import Replicator.*
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  var replicatedSet = Set.empty[Long]

  var _seqCounter = 0L


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r@Replicate(key, valueOption, id) =>
      if (!replicatedSet.contains(id)) {
        acks.find { case (_, (_, r)) => r.id == id } match {
          case Some(seq, _) =>
            replica ! Snapshot(key, valueOption, seq)
          case None =>
            acks = acks + (_seqCounter -> (sender(), r))
            replica ! Snapshot(key, valueOption, _seqCounter)
            _seqCounter += 1
        }
        context.system.scheduler.scheduleOnce(100.millis) { self ! r }
      }
    case SnapshotAck(key, seq) =>
      acks.get(seq) match
        case Some((sender, r)) =>
          sender ! Replicated(key, r.id)
          replicatedSet += r.id
        case _ =>
  }
