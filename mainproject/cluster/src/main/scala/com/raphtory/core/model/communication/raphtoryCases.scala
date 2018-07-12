package com.raphtory.core.model.communication

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.utils.CommandEnum

import scala.collection.mutable
import com.raphtory.core.model.graphentities.RemotePos

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait EmptyRaphCaseClass {
}
sealed trait RaphCaseClass extends EmptyRaphCaseClass {
  def srcId:Int
}

case class Command(command: CommandEnum.Value, value: RaphCaseClass)

case class RouterUp(id:Int)
case class PartitionUp(id:Int)
case class ClusterStatusRequest()
case class ClusterStatusResponse(clusterUp: Boolean)

//The following block are all case classes (commands) which the manager can handle
case class LiveAnalysis(analyser: Analyser)
case class Results(result:Object)

case class VertexAdd(msgTime:Long, override val srcId:Int) extends RaphCaseClass //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgTime:Long, override val srcId:Int, properties: Map[String,String]) extends RaphCaseClass
case class VertexUpdateProperties(msgTime:Long,srcId:Int, propery:Map[String,String]) extends RaphCaseClass
case class VertexRemoval(msgTime:Long,srcId:Int) extends RaphCaseClass

case class EdgeAdd(msgTime:Long,srcId:Int,dstId:Int) extends RaphCaseClass
case class EdgeAddWithProperties(msgTime:Long, override val srcId:Int,dstId:Int, properties: Map[String,String]) extends RaphCaseClass
case class EdgeUpdateProperties(msgTime:Long,srcId:Int,dstId:Int,property:Map[String,String]) extends RaphCaseClass
case class EdgeRemoval(msgTime:Long,srcId:Int,dstID:Int) extends RaphCaseClass
case class EdgeUpdateProperty(msgTime : Long, edgeId : Long, key : String, value : String) extends  EmptyRaphCaseClass
case class RemoteEdgeUpdateProperties(msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String]) extends RaphCaseClass
case class RemoteEdgeAdd(msgTime:Long, srcId:Int, dstId:Int, properties: Map[String,String]) extends RaphCaseClass
case class RemoteEdgeRemoval(msgTime:Long,srcId:Int,dstId:Int) extends RaphCaseClass

case class RemoteEdgeUpdatePropertiesNew(msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String],kills:mutable.TreeMap[Long, Boolean]) extends RaphCaseClass
case class RemoteEdgeAddNew(msgTime:Long,srcId:Int,dstId:Int,properties: Map[String,String],kills:mutable.TreeMap[Long, Boolean]) extends RaphCaseClass
case class RemoteEdgeRemovalNew(msgTime:Long,srcId:Int,dstId:Int,kills:mutable.TreeMap[Long, Boolean]) extends RaphCaseClass

case class RemoteReturnDeaths(msgTime:Long,srcId:Int,dstId:Int,kills:mutable.TreeMap[Long, Boolean]) extends RaphCaseClass
case class ReturnEdgeRemoval(msgTime:Long,srcId:Int,dstId:Int) extends RaphCaseClass

case class UpdatedCounter(newValue : Int)
case class AssignedId(id : Int)
case class PartitionsCount(count : Int)
case class RequestPartitionId()
case class RequestRouterId()


sealed trait RaphReadClasses

case class Setup(analyzer : Analyser) extends RaphReadClasses
case class Ready() extends RaphReadClasses
case class NextStep(analyzer : Analyser) extends RaphReadClasses
case class EndStep(results : Any) extends RaphReadClasses // TODO Define results
case class GetNetworkSize() extends RaphReadClasses
case class NetworkSize(size : Int) extends RaphReadClasses

case class UpdateEdgeLocation(edgeId : Long, remotePos: RemotePos.Value, remotePM : Int)
case class LocLock(entityId : Long)
case class LocUnLock(entityId : Long)
case class LocLockAck(entityId : Long)
//case class WatchDogIp(ip: String)

