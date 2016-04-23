/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{NarrowDependency, Partition, TaskContext, InterruptibleIterator}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd._
import org.apache.spark.util.Utils

import scala.collection.parallel._
import scala.collection.parallel.mutable._

import collection.mutable.HashMap

//experimental

trait Splittable[A] {

  def id : Int

  def splitIndex(a:A) : Int

  def contains(a:A) : Boolean

  def splitPar(level:Int,data:RDD[A]) : ParArray[_ <: Splittable[A]]

  def split(level:Int,data:RDD[A]) : Array[_ <: Splittable[A]]

}

//TODO add support for traverse operator !!
//TODO fix arguments ...
//TODO aggregate funtion remove ??
class HierRDDv2[T: ClassTag](
  @transient prev: RDD[T],
  val s: Splittable[T],
  coal:Boolean = false,
  val level: Int = 0) extends RDD[T](prev) with Serializable{

  override def getPartitions = prev.partitions

  val npartitions = getPartitions.size

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(split,context)
  }

  def splitPar() = {
    // split2().par
    val sp =  s.split(level,this)
    val pa = sp.zipWithIndex.map{ case (a,i) => {
      var flter = filter(e =>s.splitIndex(e)==i)
      if(coal==true && npartitions>2) flter = flter.coalesce( math.max(2,npartitions/2))

      new HierRDDv2[T](flter,sp(i),level=level+1)
    }
    }.par
    pa.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(pa.size))
    pa
  }

  def split2() = {
    val sp = s.split(level,this)
    sp.zipWithIndex.map{ case(subs,i) => {
      var flter = filter(e =>s.splitIndex(e)==i)
      if(coal==true && npartitions>2) flter = flter.coalesce( math.max(2,npartitions/2))

      new HierRDDv2[T](flter,subs,level=level+1)
    }}.filter( rdd => !rdd.isEmpty())
  }

  def prune(i:Int) = {
    split2()(i)
  }

  def exhaust(thres:Int) : Array[HierRDDv2[T]] = {
    if(level<thres) split2().flatMap( r => r.exhaust(thres) )
    else Array(this)
  }

}

///////////////////////////////////////////////
//  BACK UP FUNCTIONS
///////////////////////////////////////////////
object  hierTypes {

  type partfType[T,S] = (T,S) => Int

  type aggrfType[T,A] = (Seq[T]) => A

  type advfType[S] = (Int,S) => S
}

import hierTypes._


private[spark] class valHierPartition[T, S, A](
  @transient rdd: RDD[T],
  idx: Int,
  val parentIdx: Int,
  val values: Seq[T], //maybe not ?
  val s: S,
  val a: A) extends Partition {

  // val parent : Partition = rdd.partitions(parentIdx)

  override val index = idx
  def iterator: Iterator[T] = values.iterator

}


private[spark] class rddHierPartition[T, S, A](
  @transient rdd: RDD[T],
  idx: Int,
  val parentIndices: Array[Int],
  val s: S) extends Partition with Logging {

  // val parents = rdd.partitions(parentIndex)
  logInfo(s"new rddHierPartition $idx $rdd"+parentIndices.toString)
  // val parents  = parentIndices.map( rdd.partitions(_) )
  val parents = rdd.partitions

  override val index = idx
  // def iterator: Iterator[T] = values.iterator

}


private[spark] class groupByHierDependency[T: ClassTag, S:ClassTag, A:ClassTag](
  rdd: RDD[T],
  partitionFunc: partfType[T,S],
  advanceFunc: advfType[S],
  aggregateF: aggrfType[T,A],
  maxp: Int,
  s: S,
  parentIdx: Int)
    extends NarrowDependency[T](rdd) {

  private def f(idx: Int, iter:Iterator[T]): Iterator[(Int, T)] = {
    var res = List[(Int, T)]()
    while (iter.hasNext) {
      val cur = iter.next
      res = res ::: List((partitionFunc(cur, s), cur))
    }
    res.iterator
  }

  //TODO add filter support ( for 1 ... max filter )
  val partitions: Array[Partition] = {
    require( maxp > 0 )

    val buckets = Array.tabulate(maxp)(i => Seq[T]())
    rdd.mapPartitionsWithIndex(f).groupByKey.collect.foreach { case(idx, cb) => buckets(idx) = cb.toSeq }
    buckets.zipWithIndex.map{ case(seq,idl) => new valHierPartition(rdd,idl, parentIdx, seq, advanceFunc(idl, s), aggregateF(seq)) : Partition }
  }

  override def getParents(partitionId: Int): List[Int] = List(parentIdx)
}


/**
  *  Say something...
  */
private[spark] class lazyHierDependency[T: ClassTag, S:ClassTag, A:ClassTag](
  rdd: RDD[T],
  partitionFunc: partfType[T,S],
  advanceFunc: advfType[S],
  aggregateF: aggrfType[T,A],
  maxp: Int,
  s: S,
  key:Int,
  parentIndex: Array[Int])
    extends NarrowDependency[T](rdd) {

  //TODO add filter support ( for 1 ... max filter )
  // val partitions: Array[Partition]  = Array.tabulate(maxp)(
  // i => new rddHierPartition(rdd,i, parentIndex, advanceFunc(i, s) ) : Partition)

  val partitions: Array[Partition] = Array( new rddHierPartition(rdd,key,parentIndex,advanceFunc(key,s) ):Partition)
  override def getParents(partitionId: Int): Seq[Int] = parentIndex
  // override def getParents(partitionId: Int): Seq[Int] = parentIndex.map(rdd.partitions(_)).map(_.index).toSeq
  // override def getParents(partitionId: Int): Seq[Int] = Seq(rdd.partitions(parentIndex).index)
}

/**
  *  :: DeveloperApi ::
  * 
  *  Say something
  */
@DeveloperApi
class HierRDD[T: ClassTag, S: ClassTag,A: ClassTag](
  @transient prev: RDD[T],
  partitionFunc: partfType[T,S],
  advanceFunc: advfType[S],
  aggregateF: aggrfType[T,A],
  maxp: Int,
  keyI:Int = 0,
  val s: S,
  val depth: Int = 5,
  val level: Int = 0)
    extends RDD[T](prev.context, Nil) with Logging {

  require(level <= depth)

  // private var children_ : Array[HierRDD[T, S, A]] = null

  // private def children = {
  //   if (children_ == null)
  //     children_ = Array.tabulate(maxp)(i => {
  //       val child = filter(e => partitionFunc(e, s) == i)
  //       new HierRDD(child, partitionFunc, advanceFunc, aggregateF, maxp,
  //                   advanceFunc(i, s), parentIdx, level = level + 1)
  //    })
  //  children_
  // }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    // new InterruptibleIterator(context, split.asInstanceOf[rddHierPartition[T, S, A]].iterator)
    // OR
    // requidre(level > 0)
    if(level == 0){
      firstParent[T].iterator(split,context)
    }else if(level == 1) {
      logInfo(s"entering the compute Function $this in level $level $split")
      val parent = split.asInstanceOf[rddHierPartition[T, S, A]].parents
      // require(parent.size ==1)
      parent.iterator.flatMap{ p =>
        firstParent[T].iterator(p,context)
      }
    } else {
      logInfo("--HDEBUG LEVEL > 1 ")
      val parent = split.asInstanceOf[rddHierPartition[T, S, A]].parents
      require(parent.size==1)
      firstParent[T].compute(parent(0),context)
      // logInfo(s"--HDEBUG compute $split $level")
    }
    // if(level > 0 ) firstParent[T].iterator(parentp,context).filter(e => partitionFunc(e,s) == split.index)
    // else
  }

  // override def compute(split: Partition, context: TaskContext): Iterator[T] = {
  //   new InterruptibleIterator(context, split.asInstanceOf[HierRDDPartition[T, S, A]].iterator)
  // }
  override def getDependencies: Seq[Dependency[_]] = {
    if(level==0) Seq( new NarrowDependency(prev) {
      // def getParents(id:Int): Seq[Int] = Seq(prev.partitions(id).index)
      def getParents(id:Int): Seq[Int] = prev.partitions.map(_.index) //nodifference ???
    })
    else if(level==1){ Seq( new lazyHierDependency[T, S, A](
      prev, partitionFunc, advanceFunc, aggregateF, maxp, s, keyI, prev.partitions.map(_.index).toArray ))
    } else prev.partitions.map(p => new lazyHierDependency[T, S, A](
      prev, partitionFunc, advanceFunc, aggregateF, maxp, s, keyI, Array(p.index))).toSeq
    // Seq(new lazyHierDependency(prev) {
    //   def getParents(id: Int): Seq[Int] =
    //     partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
    // })
  }

  override protected def getPartitions: Array[Partition] = {
    if(level > 0)  getDependencies.head.asInstanceOf[lazyHierDependency[T, S, A]].partitions
    // if(level > 0) Array.tabulate(maxp)(
    // i => new rddHierPartition(this,i, prev.partitions.map(_.index).toArray, advanceFunc(i, s) ) : Partition)
    else firstParent[T].partitions
  }
  // override def clearDependencies() {
  //   super.clearDependencies()
  //   prev = null
  // }

  def prune(key: Int) = {
    val child = filter(e => partitionFunc(e, s) == key)
    new HierRDD(child, partitionFunc, advanceFunc, aggregateF, maxp, key, advanceFunc(key, s), level = level + 1)
  }

  // def prune(parentIdx: Int) = children(parentIdx) // throws exception in case of illegal parentidx

  def split: Array[HierRDD[T, S, A]] = {
    partitions.foreach( p => logInfo(s"--DEBUG HRDD split called partiiton $p LEVEL($level) DEPTH($depth)"))
    if(level == 0) Array.range(0,maxp).map( n => prune(n) )
    // else if (level < depth) partitions.map(p => prune(p.index))
    if(level < depth) Array.range(0,maxp).map( n => prune(n) )
    else Array[HierRDD[T, S, A]](this)
  }

    /*
  def exhaust = {
    def iter(rdd: HierRDD[T, S, A], level: Int, l: List[Any]): List[Any] = {
      if (level == depth) List(rdd)
      else l ++: rdd.split.map(r => iter(r, level + 1, l)).toList
    }
    iter(this, level, List())
     }*/
  }
