package com.raphtory.examples.partitioning

import com.raphtory.core.model.PartitioningTrait
import com.raphtory.core.model.graphentities.RemoteEdge
import com.raphtory.core.storage.EntitiesStorage

import scala.collection.parallel.mutable.ParTrieMap

class LinearGreedy extends PartitioningTrait {
  val C = Math.pow(2, 10) // TODO has this to be set here or in the PartitionWriter class?
  def w(i : Int)(implicit partSizeMap : ParTrieMap[Int, Long]) = 1 - partSizeMap.get(i).get / C
  override def applyPartitioning(entityId: Long, managerCount: Int, managerID: Int)(implicit storage: EntitiesStorage.type,  partSizeMap : ParTrieMap[Int, Long]): Int = {
    // Compute the best partitionManager to be in charge of the local entity entityId and return the managerId to which the entity has to be migrated
    var max : Double = 0
    var nextPartition : Int = -1
    (0 to managerCount).foreach(i => {
      if (i != managerID) {
        storage.vertices.get(entityId.toInt) match {
          case Some(v) =>
            val weight =
              v.associatedEdges
              .filter(e =>
                e.isInstanceOf[RemoteEdge] && e.asInstanceOf[RemoteEdge].remotePartitionID == i)
              .size * w(i)

            // the filter get the set made by the intersection of the set of vertices stored in partition $i and the v's neighbors stored in the same partition

            if (weight > max) {
              max = weight
              nextPartition = i
            }
          case None =>
        }
      }
    })
    nextPartition
  }

}
