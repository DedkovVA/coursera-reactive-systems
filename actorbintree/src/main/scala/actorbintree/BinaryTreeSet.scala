/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*

import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation =>
      root ! op
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation =>
      pendingQueue = pendingQueue :+ op
    case CopyFinished =>
      context.become(normal)
      pendingQueue.foreach(op => newRoot ! op)
      root = newRoot
      pendingQueue = Queue.empty
    case GC =>
  }


object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def doOperation(elem: Int, ifEq: => Unit, ifSome: ActorRef => Unit, ifNoneL: => Unit, ifNoneR: => Unit) = {
    if (this.elem == elem) {
      ifEq
    } else if (elem < this.elem) {
      subtrees.get(Left) match {
        case Some(value) =>
          ifSome(value)
        case None =>
          ifNoneL
      }
    } else {
      subtrees.get(Right) match {
        case Some(value) =>
          ifSome(value)
        case None =>
          ifNoneR
      }
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, elem) =>
      def q(pos: Position) = {
        subtrees = subtrees + (pos -> context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false)))
        requester ! OperationFinished(id)
      }
      doOperation(
        elem,
        ifEq = {
          if (removed) removed = false
          requester ! OperationFinished(id)
        },
        ifSome = _ ! Insert(requester, id, elem),
        ifNoneL = q(Left),
        ifNoneR = q(Right)
      )
    case Contains(requester, id, elem) =>
      def ifNone = requester ! ContainsResult(id, result = false)
      doOperation(
        elem,
        ifEq = requester ! ContainsResult(id, result = !removed),
        ifSome = _ ! Contains(requester, id, elem),
        ifNoneL = ifNone,
        ifNoneR = ifNone
      )
    case Remove(requester, id, elem) =>
      def ifNone = requester ! OperationFinished(id)
      doOperation(
        elem,
        ifEq = {
          removed = true
          requester ! OperationFinished(id)
        },
        ifSome = _ ! Remove(requester, id, elem),
        ifNoneL = ifNone,
        ifNoneR = ifNone
      )
    case copyTo@CopyTo(newRoot) =>
      context.become(copying(0, insertConfirmed = false))
      subtrees.values.foreach(node => node ! copyTo)
      if (removed) {
        self ! OperationFinished(elem)
      } else {
        newRoot ! Insert(self, elem, elem)
      }
  }

  def copyingF(childrenCopiedNum: Int, insertConfirmed: Boolean) = {
    if (subtrees.size == childrenCopiedNum) {
      subtrees.values.foreach(_ ! PoisonPill)
      self ! PoisonPill
      context.parent ! CopyFinished
    } else {
      context.become(copying(childrenCopiedNum, insertConfirmed))
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  //def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
  def copying(childrenCopiedNum: Int, insertConfirmed: Boolean): Receive = {
    case _: OperationFinished =>
      copyingF(childrenCopiedNum, insertConfirmed = true)
    case CopyFinished =>
      copyingF(childrenCopiedNum + 1, insertConfirmed = insertConfirmed)
  }


