package com.raphtory.core.model.communication

import com.raphtory.api.Analyser
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.graphentities.Edge

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait Property {
  def key: String
  def value: Any
}

case class Type(name: String)
case class ImmutableProperty(key: String, value: String) extends Property
case class StringProperty(key: String, value: String)    extends Property
case class LongProperty(key: String, value: Long)        extends Property
case class DoubleProperty(key: String, value: Double)    extends Property
case class Properties(property: Property*)

sealed trait GraphUpdate {
  val updateTime: Long
  val srcId: Long
}

case class VertexAdd(updateTime: Long, srcId: Long, properties: Properties, vType: Option[Type]) extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)
case class VertexDelete(updateTime: Long, srcId: Long) extends GraphUpdate
case class EdgeAdd(updateTime: Long, srcId: Long, dstId: Long, properties: Properties, eType: Option[Type]) extends GraphUpdate
case class EdgeDelete(updateTime: Long, srcId: Long, dstId: Long) extends GraphUpdate

case class TrackedGraphUpdate[+T <: GraphUpdate](channelId: String, channelTime:Int, update: T)

sealed abstract class GraphEffect(val targetId: Long) extends Serializable

case class RemoteEdgeAdd(msgTime: Long, srcID: Long, dstID: Long, properties: Properties, eType: Type, routerID: String, routerTime: Int) extends GraphEffect(dstID)
case class RemoteEdgeRemoval(msgTime: Long, srcID: Long, dstID: Long, routerID: String, routerTime: Int) extends GraphEffect(dstID)
case class RemoteEdgeRemovalFromVertex(msgTime: Long, srcID: Long, dstID: Long, routerID: String, routerTime: Int) extends GraphEffect(dstID)

case class RemoteEdgeAddNew(msgTime: Long, srcID: Long, dstID: Long, properties: Properties, kills: mutable.TreeMap[Long, Boolean], vType: Type, routerID: String, routerTime: Int) extends GraphEffect(dstID)
case class RemoteEdgeRemovalNew(msgTime: Long, srcID: Long, dstID: Long, kills: mutable.TreeMap[Long, Boolean], routerID: String, routerTime: Int) extends GraphEffect(dstID)

case class RemoteReturnDeaths(msgTime: Long, srcID: Long, dstID: Long, kills: mutable.TreeMap[Long, Boolean], routerID: String, routerTime: Int) extends GraphEffect(srcID)
case class ReturnEdgeRemoval(msgTime: Long, srcID: Long, dstID: Long,routerID:String,routerTime:Int) extends GraphEffect(srcID)

//BLOCK FROM WORKER SYNC
case class DstAddForOtherWorker(msgTime: Long, dstID: Long, srcForEdge: Long, edge: Edge, present: Boolean, routerID: String, routerTime: Int) extends GraphEffect(dstID)
case class DstWipeForOtherWorker(msgTime: Long, dstID: Long, srcForEdge: Long, edge: Edge, present: Boolean, routerID: String, routerTime: Int) extends GraphEffect(dstID)
case class DstResponseFromOtherWorker(msgTime: Long, srcID: Long, dstID: Long, removeList: mutable.TreeMap[Long, Boolean], routerID: String, routerTime: Int) extends GraphEffect(srcID)
case class EdgeRemoveForOtherWorker(msgTime: Long, srcID: Long, dstID: Long,routerID: String, routerTime: Int) extends GraphEffect(srcID)

case class EdgeSyncAck(msgTime: Long, srcID: Long, routerID: String, routerTime: Int) extends GraphEffect(srcID)
case class VertexRemoveSyncAck(msgTime: Long, override val targetId: Long, routerID: String, routerTime: Int) extends GraphEffect(targetId)
case class RouterWorkerTimeSync(msgTime:Long,routerID:String,routerTime:Int)


sealed trait RaphReadClasses

case class VertexMessage(vertexID: Long, viewJob: ViewJob, superStep: Int, data:Any )


case class Setup(analyzer: Analyser, jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class SetupNewAnalyser(jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class Ready(messages: Int) extends RaphReadClasses
case class NextStep(analyzer: Analyser, jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class NextStepNewAnalyser(jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class EndStep(messages: Int, voteToHalt: Boolean) extends RaphReadClasses
case class Finish(analyzer: Analyser, jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class FinishNewAnalyser(jobID: String, args:Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) extends RaphReadClasses
case class ReturnResults(results: Any)
case class ExceptionInAnalysis(e: String) extends RaphReadClasses

case class MessagesReceived(workerID: Int, receivedMessages: Int, sentMessages: Int) extends RaphReadClasses
case class CheckMessages(jobID:ViewJob,superstep: Int) extends RaphReadClasses

case class ReaderWorkersOnline() extends RaphReadClasses
case class ReaderWorkersACK()    extends RaphReadClasses



case class LiveAnalysisPOST(analyserName:String, windowType:Option[String], windowSize:Option[Long], windowSet:Option[Array[Long]],repeatTime:Option[Long],eventTime:Option[Boolean],args:Option[Array[String]],rawFile:Option[String])
case class ViewAnalysisPOST(analyserName:String,timestamp:Long,windowType:Option[String],windowSize:Option[Long],windowSet:Option[Array[Long]],args:Option[Array[String]],rawFile:Option[String])
case class RangeAnalysisPOST(analyserName:String,start:Long,end:Long,jump:Long,windowType:Option[String],windowSize:Option[Long],windowSet:Option[Array[Long]],args:Option[Array[String]],rawFile:Option[String])
trait AnalysisRequest
case class LiveAnalysisRequest(
    analyserName: String,
    repeatTime:Long =0L,
    eventTime:Boolean=false,
    windowType: String = "false",
    windowSize: Long = 0L,
    windowSet: Array[Long] = Array[Long](0),
    args:Array[String]=Array(),
    rawFile:String=""
) extends AnalysisRequest
case class ViewAnalysisRequest(
    analyserName: String,
    timestamp: Long,
    windowType: String = "false",
    windowSize: Long = 0L,
    windowSet: Array[Long] = Array[Long](0),
    args:Array[String]=Array(),
    rawFile:String=""
) extends AnalysisRequest
case class RangeAnalysisRequest(
    analyserName: String,
    start: Long,
    end: Long,
    jump: Long,
    windowType: String = "false",
    windowSize: Long = 0L,
    windowSet: Array[Long] = Array[Long](0),
    args:Array[String]=Array(),
    rawFile:String=""
) extends AnalysisRequest

case class AnalyserPresentCheck(className: String)            extends RaphReadClasses
case class AnalyserPresent()                                  extends RaphReadClasses
case class ClassMissing()                                     extends RaphReadClasses
case class FailedToCompile(stackTrace: String)                extends RaphReadClasses
case class CompileNewAnalyser(analyser: String,args:Array[String], name: String) extends RaphReadClasses
case class ClassCompiled()                                    extends RaphReadClasses
case class TimeCheck(timestamp: Long)                         extends RaphReadClasses
case class TimeResponse(ok: Boolean, time: Long)              extends RaphReadClasses
case class RequestResults(jobID:String)
case class KillTask(jobID:String)
case class JobKilled()
case class ResultsForApiPI(results:Array[String])
case class JobDoesntExist()
case class AllocateTuple(record: Any)