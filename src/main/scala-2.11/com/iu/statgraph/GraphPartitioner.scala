package com.iu.statgraph

import com.iu.statgraph.Main.{Edge, Vertex}
import org.apache.spark.Partitioner

/**
  * @author Kamil Salahiev on 19/08/16
  */
class GraphPartitioner(partitions: Int) extends Partitioner{
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
        key match {
            case vertex: Vertex => vertex.id.hashCode() % numPartitions
            case edge: Edge => edge.id1.hashCode() % numPartitions
            case _ => throw new RuntimeException("Wrong type is passed")
        }
    }
}
