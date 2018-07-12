package com.raphtory.core.actors.partitionmanager

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import com.raphtory.core.storage.EntitiesStorage
import com.raphtory.core.model.graphentities._
import com.raphtory.core.model.communication._
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.utils.Utils
import kamon.Kamon
import kamon.metric.GaugeMetric
import monix.eval.{Fiber, Task}
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler

import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class PartitionWriter(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  var managerCount : Int = managerCountVal
  val managerID    : Int = id                   //ID which refers to the partitions position in the graph manager map

  val printing: Boolean = false                  // should the handled messages be printed to terminal
  val kLogging: Boolean = System.getenv().getOrDefault("PROMETHEUS", "true").trim().toBoolean // should the state of the vertex/edge map be output to Kamon/Prometheus
  val stdoutLog:Boolean = System.getenv().getOrDefault("STDOUT_LOG", "false").trim().toBoolean // A slower logging for the state of vertices/edges maps to Stdout

  val messageCount          : AtomicInteger = new AtomicInteger(0)         // number of messages processed since last report to the benchmarker
  val secondaryMessageCount : AtomicInteger = new AtomicInteger(0)


  val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  val storage = EntitiesStorage.apply(printing, managerCount, managerID, mediator)

  mediator ! DistributedPubSubMediator.Put(self)

  val verticesGauge : GaugeMetric = Kamon.gauge("raphtory.vertices")
  val edgesGauge    : GaugeMetric = Kamon.gauge("raphtory.edges")

  final val locMap = ParTrieMap[Long, Int]()
  final val locMapSemaphore = ParSet[Long]()
  final val migrationJobs = ParTrieMap[Long, Unit]()

  // TODO get the threshold dynamically managed
  val migrationThreshold = 1024

  def applyPartitioningStrategy(entityId : Long) : Int = {
    // Compute the best partitionManager to be in charge of the local entity entityId and return the managerId to which the entity has to be migrated
    0
  }

  def executeMigrationJobs = {
    migrationJobs.foreach(el => {
      val entityId = el._1
      val callback = el._2
      val managerId = applyPartitioningStrategy(entityId)
      val hashingManagerID = Utils.getPartition(entityId.toInt, managerCount)

      implicit val timeout = akka.util.Timeout(FiniteDuration(10, SECONDS))
      var v : Vertex = null

      if (managerId != this.managerID) {
        locMap.remove(entityId) // Remove the element from the locMap if present (in the case this partitionManager isn't the one corresponding to the hash strategy
        if (this.managerID != hashingManagerID) {
          // This is a migration for an already migrated vertex
          val x = mediator ? DistributedPubSubMediator.Send(Utils.getManagerUri(hashingManagerID), LocLock(entityId), false)
          Await.ready(x, Duration.Inf).value.get match {
            case Success(t) =>
              t match {
                case LocLockAck =>
                case _ => {
                  println("ERROR ON DISTRIBUTED LOCK")
                  throw new Exception()
                }
              }
            case Failure(e) =>
              println("FAILURE ON DISTRIBUTED LOCK")
              throw new Exception()
          }

        } else {
          locMapSemaphore.+(entityId)
        }
          // migrate vertex and associatedEdges
          v = storage.vertices.remove(entityId.toInt).get
          mediator ! DistributedPubSubMediator.Send(Utils.getManagerUri(managerId), v, false)
        if (this.managerID != hashingManagerID) {
          // This is a migration for an already migrated vertex
          mediator ? DistributedPubSubMediator.Send(Utils.getManagerUri(hashingManagerID), LocUnLock(entityId), false)
        } else {
          locMapSemaphore.-(entityId)
        }
        v.associatedEdges.filter(e => !e.isInstanceOf[RemoteEdge]).foreach(e => {
          // local edges have to be kept and updated to remote edges with the proper destination
          if (e.getSrcId == v.getId) {
            val re = RemoteEdge(e.creationTime, e.getId, e.previousState, e.properties, RemotePos.Destination, managerId)
            storage.edges.synchronized {
              storage.edges.remove(e.getId)
              storage.edges.put(e.getId, re)
              val otherSideVertex = storage.vertices.get(e.getDstId).get
              otherSideVertex.associatedEdges.-(e)
              otherSideVertex.associatedEdges.+(re)
            }
          }
        })
        // remote edges have to be dropped (just this end was located in the current PM, so they have not to stay here)
        v.associatedEdges.filter(e => e.isInstanceOf[RemoteEdge]).foreach(e => storage.edges.remove(e.getId))
        }
      })
  }

  def applyRequestInnerThread[A, B](entityId : Long, callback : => Task[Fiber[B]], onComplete : () => Unit, toBeForwarded : EmptyRaphCaseClass) = {
    while (locMapSemaphore.contains(entityId)) {
      Thread.sleep(100) // TODO Look at performance without sleeping
    }

    locMap.get(entityId) match {
      case None => callback.runAsync(s)
      case Some(partitionManager) => {
        mediator ! DistributedPubSubMediator.Send(Utils.getManagerUri(partitionManager), toBeForwarded, false)
        migrationJobs.put(entityId, () => applyPartitioningStrategy(entityId))
        if (migrationJobs.size > migrationThreshold) {
          Task.eval(executeMigrationJobs).fork.runAsync(s)
        }
      }
    }
  }

  def applyRequest[A, B](entityId : Long, callback : => Task[Fiber[B]], onComplete : () => Unit, toBeForwarded : EmptyRaphCaseClass) =
    Task.eval(applyRequestInnerThread(entityId, callback, onComplete, toBeForwarded)).fork.runAsync


  //implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))

  // Explicit execution model
  implicit val s = Scheduler.io(
    name="my-io",
    executionModel = AlwaysAsyncExecution
  )
  // Simple constructor
  //implicit val s =
  //  Scheduler.computation(parallelism=16)

  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS), self, "tick")
    //context.system.scheduler.schedule(Duration(13, SECONDS),
    //    Duration(30, MINUTES), self, "profile")
    context.system.scheduler.schedule(Duration(1, SECONDS),
        Duration(1, MINUTES), self, "stdoutReport")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }

  override def receive : Receive = {
    case "tick"         => reportIntake()
    case "profile"      => profile()
    case "keep_alive"   => keepAlive()
    case "stdoutReport" => Task.eval(reportStdout()).fork.runAsync

    //case LiveAnalysis(name,analyser) => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)

    case VertexAdd(msgTime,srcId)     =>
      applyRequest(srcId, Task.eval(storage.vertexAdd(msgTime,srcId)).fork,() => vHandle(srcId), VertexAdd(msgTime,srcId))

    case VertexRemoval(msgTime,srcId) =>
      applyRequest(srcId, Task.eval(storage.vertexRemoval(msgTime,srcId)).fork,
        () => vHandle(srcId), VertexRemoval(msgTime,srcId))

    case VertexAddWithProperties(msgTime,srcId,properties) =>
      applyRequest(srcId, Task.eval(storage.vertexAdd(msgTime,srcId,properties)).fork,
        () => vHandle(srcId),VertexAddWithProperties(msgTime,srcId,properties))

    case EdgeAdd(msgTime,srcId,dstId) =>
      applyRequest(Utils.getEdgeIndex(srcId,dstId), Task.eval(storage.edgeAdd(msgTime,srcId,dstId)).fork,
        () => eHandle(srcId,dstId),EdgeAdd(msgTime,srcId,dstId))

    case RemoteEdgeAdd(msgTime,srcId,dstId,properties) =>
      applyRequest(Utils.getEdgeIndex(srcId,dstId),
        Task.eval(storage.remoteEdgeAdd(msgTime,srcId,dstId,properties)).fork,() => eHandleSecondary(srcId,dstId),
        RemoteEdgeAdd(msgTime,srcId,dstId,properties))

    case RemoteEdgeAddNew(msgTime,srcId,dstId,properties,deaths) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId),
        Task.eval(storage.remoteEdgeAddNew(msgTime,srcId,dstId,properties,deaths)).fork,
        () => eHandleSecondary(srcId,dstId), RemoteEdgeAddNew(msgTime,srcId,dstId,properties,deaths))

    case EdgeAddWithProperties(msgTime,srcId,dstId,properties) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId),
        Task.eval(storage.edgeAdd(msgTime,srcId,dstId,properties)).fork,() => eHandle(srcId,dstId),EdgeAddWithProperties(msgTime,srcId,dstId,properties))

    case EdgeRemoval(msgTime,srcId,dstId) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId), Task.eval(storage.edgeRemoval(msgTime,srcId,dstId)).fork,() => eHandle(srcId,dstId),EdgeRemoval(msgTime,srcId,dstId))

    case RemoteEdgeRemoval(msgTime,srcId,dstId) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId),
        Task.eval(storage.remoteEdgeRemoval(msgTime,srcId,dstId)).fork,
        () => eHandleSecondary(srcId,dstId), RemoteEdgeRemoval(msgTime,srcId,dstId))
    case RemoteEdgeRemovalNew(msgTime,srcId,dstId,deaths) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId),
        Task.eval(storage.remoteEdgeRemovalNew(msgTime,srcId,dstId,deaths)).fork,() =>
          eHandleSecondary(srcId,dstId),RemoteEdgeRemovalNew(msgTime,srcId,dstId,deaths))

    case ReturnEdgeRemoval(msgTime,srcId,dstId) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId), Task.eval(storage.returnEdgeRemoval(msgTime,srcId,dstId)).fork,
        () => eHandleSecondary(srcId,dstId),ReturnEdgeRemoval(msgTime,srcId,dstId))

    case RemoteReturnDeaths(msgTime,srcId,dstId,deaths) =>
      applyRequest(Utils.getEdgeIndex(srcId, dstId), Task.eval(storage.remoteReturnDeaths(msgTime,srcId,dstId,deaths)).fork,
        () => eHandleSecondary(srcId,dstId),RemoteReturnDeaths(msgTime,srcId,dstId,deaths))

    case v : Vertex => {
      storage.vertices.put(v.getId.toInt, v)
      v.associatedEdges.foreach(e => {
        if (storage.edges.contains(e.getId)) { // edge has become local
          val oldEdge = storage.edges.get(e.getId).get
          val newLocalEdge = Edge(e.creationTime, e.getId, e.previousState, e.properties)
          v.associatedEdges.-(e)
          v.associatedEdges.+(newLocalEdge)
          storage.edges.put(newLocalEdge.getId, newLocalEdge)
          var otherSideVertex : Vertex = null
          if (newLocalEdge.getSrcId == v.getId) {
            // v is the source and edge is now local
            otherSideVertex = storage.vertices.get(newLocalEdge.getDstId).get
          } else {
            // v is the destination and edge is now local
            otherSideVertex = storage.vertices.get(newLocalEdge.getDstId).get
          }
          // Edge is now local, updating otherSide vertex
          otherSideVertex.associatedEdges.-(oldEdge)
          otherSideVertex.associatedEdges.+(newLocalEdge)

        } else { // edge is (or has become) remote
          if (e.isInstanceOf[RemoteEdge]) { // It was Remote
            if (e.getSrcId == v.getId) { // v is the source vertex, destination remains the same, need update on destination vertex

              // send message to destination to update with this partitionManager id the source remote position on the ghost edge
              mediator ! DistributedPubSubMediator.Send(Utils.getManagerUri(e.asInstanceOf[RemoteEdge].remotePartitionID), UpdateEdgeLocation(e.getId, RemotePos.Source, managerID), false)
            } else { // v is the destination vertex
              // send message to source to update with this partitionManager id the destination remote position on the main edge
              mediator ! DistributedPubSubMediator.Send(Utils.getManagerUri(e.asInstanceOf[RemoteEdge].remotePartitionID), UpdateEdgeLocation(e.getId, RemotePos.Destination, managerID), false)

            }

          } else { // It was local
            // create a new remote edge to be stored in this partition manager, the other side was converted before during the vertex migration process
            var newEdge : RemoteEdge = null
            if (e.getSrcId == v.getId) { // This is the main edge
              newEdge = RemoteEdge(e.creationTime, e.getId, e.previousState, e.properties,
                RemotePos.Destination, sender().path.toStringWithoutAddress.replaceFirst("^.*Manager_", "").toInt)
            } else { // This is the ghost-copy edge
               newEdge = RemoteEdge(e.creationTime, e.getId, e.previousState, e.properties,
                RemotePos.Source, sender().path.toStringWithoutAddress.replaceFirst("^.*Manager_", "").toInt)
            }
            v.associatedEdges.-(e)
            v.associatedEdges.+(newEdge)
            storage.edges.put(newEdge.getId, newEdge)
          }
        }
        storage.edges.put(e.getId, e)
      })
    }

    case UpdateEdgeLocation(edgeId, remotePos, remotePM) =>
      val e = storage.edges.get(edgeId).get.asInstanceOf[RemoteEdge]
      e.remotePartitionID = remotePM
      e.remotePos = remotePos

    case UpdatedCounter(newValue) => {
      managerCount = newValue
      storage.setManagerCount(managerCount)
    }
    case EdgeUpdateProperty(msgTime, edgeId, key, value)        =>
      applyRequest(edgeId, Task.eval(storage.updateEdgeProperties(msgTime, edgeId, key, value)).fork,
        () => {}, EdgeUpdateProperty(msgTime, edgeId, key, value))
    case e => println(s"Not handled message ${e.getClass} ${e.toString}")
 }

  def keepAlive() : Unit = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), localAffinity = false)

  /*****************************
   * Metrics reporting methods *
   *****************************/
  def getEntitiesPrevStates[T,U <: Entity](m : ParTrieMap[T, U]) : Int = {
    var ret = new AtomicInteger(0)
    m.foreach[Unit](e => {
      ret.getAndAdd(e._2.getPreviousStateSize())
    })
    ret.get
  }

  def reportSizes[T, U <: Entity](g : kamon.metric.GaugeMetric, map : ParTrieMap[T, U]) : Unit = {
    def getGauge(name : String) = {
     g.refine("actor" -> "PartitionManager", "replica" -> id.toString, "name" -> name)
    }

    Task.eval(getGauge("Total number of entities").set(map.size)).fork.runAsync
    //getGauge("Total number of properties") TODO

    Task.eval(getGauge("Total number of previous states").set(
      getEntitiesPrevStates(map)
    )).fork.runAsync

    // getGauge("Number of props previous history") TODO
  }

  def reportStdout() : Unit = {
    if (stdoutLog)
      println(s"TrieMaps size: ${storage.edges.size}\t${storage.vertices.size} | " +
              s"TreeMaps size: ${getEntitiesPrevStates(storage.edges)}\t${getEntitiesPrevStates(storage.vertices)}")
  }
  def reportIntake() : Unit = {
    if(printing)
      println(messageCount)

    // Kamon monitoring
    if (kLogging) {
      kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString).set(messageCount.intValue())
      kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount", "replica" -> id.toString).set(secondaryMessageCount.intValue())
      reportSizes(edgesGauge, storage.edges)
      reportSizes(verticesGauge, storage.vertices)
    }

    // Heap benchmarking
    //profile()

    // Reset counters
    messageCount.set(0)
    secondaryMessageCount.set(0)
  }

  def vHandle(srcID : Int) : Unit = {
    messageCount.incrementAndGet()
  }

  def vHandleSecondary(srcID : Int) : Unit = {
    secondaryMessageCount.incrementAndGet()
  }
  def eHandle(srcID : Int, dstID : Int) : Unit = {
    messageCount.incrementAndGet()
  }

  def eHandleSecondary(srcID : Int, dstID : Int) : Unit = {
    secondaryMessageCount.incrementAndGet()
  }

  /*********************************
   * END Metrics reporting methods *
   *********************************/
}
