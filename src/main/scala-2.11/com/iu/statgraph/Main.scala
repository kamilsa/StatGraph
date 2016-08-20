package com.iu.statgraph

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
  * @author Kamil Salahiev on 18/08/16
  */
object Main {

    abstract class Struct(){
        override def equals(obj: scala.Any): Boolean = {
            if (obj == null) return false
            (this, obj) match {
                case (v: Vertex, e: Edge) => v.id.compare(e.id1) == 0
                case (e: Edge, v: Vertex) => v.id.compare(e.id1) == 0
                case (v1: Vertex, v2: Vertex) => v1.id == v2.id
                case (e1: Edge, e2: Edge) => e1.id1 == e2.id1
            }
        }

        override def hashCode(): Int = {
            this match {
                case v:Vertex => v.id.hashCode()
                case e:Edge => e.id1.hashCode()
            }
        }
    }

    object Struct {
        implicit def orderingBy[A <: Struct] : Ordering[A] = {
            new Ordering[A] {
                override def compare(x: A, y: A): Int = {
                    (x,y) match {
                        case (vertex: Vertex, edge: Edge) =>
                            if (vertex.id == edge.id1) -1
                            vertex.id.compare(edge.id1)

                        case (edge: Edge, vertex: Vertex) =>
                            if (vertex.id == edge.id1) 1
                            edge.id1.compare(vertex.id)

                        case (edge1: Edge, edge2: Edge) => edge1.id1.compare(edge2.id1)
                        case (vertex1: Vertex, vertex2: Vertex) => vertex1.id.compare(vertex2.id)
                    }
                }
            }
        }
    }

    case class Vertex(id: Int, d: Int) extends Struct
    case class Edge(id1: Int, id2: Int, d: Int) extends Struct

    def initSc(): (SparkContext, SQLContext) = {
        val conf = new SparkConf()
            .setAppName("Static graph processing")
            .setMaster("local[*]")
        //            .setMaster("spark://10.91.36.100:7077")
        //            .setJars(Array("lib/dbscan-0.1.jar","lib/spatialpartitioner_2.11-0.1-SNAPSHOT.jar"))
        //                    .set("spark.executor.memory", "6g")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        return (sc, sqlContext)
    }

    def getVertAndEdges(filename: String, sqlContext: SQLContext): (DataFrame, DataFrame) = {
        val edges = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", ",")
            .schema(StructType(
                "id1 id2".split(" ")
                .map(fieldname => {
                    StructField(fieldname, IntegerType, nullable = false)
                })))
            .load(filename)

        val vertices = edges
            .select("id1")
            .distinct()
            .unionAll(edges
                .select("id2")
                .distinct())
            .distinct()
            .withColumnRenamed("id1", "id")

        vertices.registerTempTable("vertices")
        edges.registerTempTable("edges")
        (vertices, edges)

    }

    def NEjoin(
          v: RDD[Tuple2[Int, Struct]],
          e: RDD[Tuple2[Int, Struct]],
          cond: (Int, Int) => Boolean,
          transform: (Int, Edge) => Edge
              ): RDD[Tuple2[Int, Struct]] = {
        v.first()._2 match {
            case edge: Edge => throw new RuntimeException("Wrong Rdd passed")
            case vertex: Vertex =>
        }
        e.first()._2 match {
            case vertex: Vertex => throw new RuntimeException("Wrong Rdd passed")
            case edge: Edge =>
        }

        v.union(e)
            .groupByKey()
            .mapValues(iter => {
                val list = iter.toList.sorted
                val v = list.head.asInstanceOf[Vertex]
                list.slice(1, list.length)
                    .map(_.asInstanceOf[Edge])
                    .filter(t => cond(v.d, t.d))
                    .map(t=> transform(v.d, t))
                    .map(t => (t.id1, t))
            })
            .flatMap(_._2)
    }

    def ENjoin(
                  v: RDD[Tuple2[Int, Struct]],
                  e: RDD[Tuple2[Int, Struct]],
                  cond: (Int, Int) => Boolean,
                  reduce: (Int, Int, Int) => Int
              ): RDD[Tuple2[Int, Struct]] = {
        v.first()._2 match {
            case edge: Edge => throw new RuntimeException("Wrong Rdd passed")
            case vertex: Vertex =>
        }
        e.first()._2 match {
            case vertex: Vertex => throw new RuntimeException("Wrong Rdd passed")
            case edge: Edge =>
        }

        v.union(e)
            .groupByKey()
            .mapValues(iter => {
                val list = iter.toList.sorted
                val v = list.head.asInstanceOf[Vertex]
                val res = list.slice(1, list.length)
                    .map(_.asInstanceOf[Edge])
                    .filter(t => cond(v.d, t.d))
                    .map(_.d)
                    .reduce((x,y) => reduce(x,y, v.d))
                val resV = Vertex(id = v.id, d = res)
                resV
            })
    }

    def bfs(
       v: RDD[Tuple2[Int, Struct]],
       e: RDD[Tuple2[Int, Struct]],
       s: Vertex): Unit = {
        val phi = Int.MaxValue
        val vd = v
            .map(t => {
                val id = t._2.asInstanceOf[Vertex].id
                (t._1, Vertex(id, d = {if (id == s.id) 0 else phi}).asInstanceOf[Struct])
            })
        var i = 1
        while (true) {
            val ed = NEjoin(
                vd,
                e,
                (vertD, edgeD) => (vertD != phi),
                (vertD, edge) =>
                    Edge(
                        id1 = edge.asInstanceOf[Edge].id1,
                        id2 = edge.asInstanceOf[Edge].id2,
                        d = vertD)
            )
        }
    }

    def main(args: Array[String]): Unit = {
        val (sc, sqlContext) = initSc()

        val (vertices, edges) = getVertAndEdges("res/graph/sample.txt", sqlContext)

        val v = vertices
            .map(row => (row.getInt(0), Vertex(id = row.getInt(0), row.getInt(0)*10).asInstanceOf[Struct]))

        val e = edges
            .map(row => (row.getInt(0), Edge(id1 = row.getInt(0), id2 = row.getInt(1), d = row.getInt(0) + row.getInt(1)).asInstanceOf[Struct]))


        def cond(i1: Int, i2: Int):Boolean = {i1*10 > i2}
        def transform(i: Int, e: Edge): Edge = {
            Edge(e.id1, e.id2, e.d * i)
        }

        def reduce(i1: Int, i2: Int, i3: Int): Int= {i3*(i1+i2)}

//        NEjoin(v,e, cond, transform)
//            .collect()
//            .foreach(println(_))
        ENjoin(v,e,cond, reduce)
            .collect()
            .foreach(println(_))
        sc.stop()
    }
}
