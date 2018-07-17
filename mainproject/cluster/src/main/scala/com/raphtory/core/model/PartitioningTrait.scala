package com.raphtory.core.model
import com.raphtory.core.storage.EntitiesStorage
import scala.collection.parallel.mutable.ParTrieMap

trait PartitioningTrait {
 def applyPartitioning(entityId : Long, managerCount : Int, managerID : Int)(implicit storage : EntitiesStorage.type, partSizeMap: ParTrieMap[Int,Long]) : Int
}
