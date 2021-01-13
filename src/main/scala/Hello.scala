import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

object Hello {
  def gSerach(x: List[Row], trace: List[List[Int]]): List[Int] = {
    val y = List.fill(x.length)(1)

    buildGrapgh(x, y) match {
      case (_, true) => backward(trace)
      case (_, false) => gSerach(x.tail, trace :+ y)
    }
  }

  def graphSearch(x: List[Row], newTrace: List[List[Int]], done: Boolean): List[Int] = {
    val y = List.fill(x.length)(1)

    if (!done) {
      val (_y, finished) = buildGrapgh(x, y)
      if (finished) {
        graphSearch(x.tail, newTrace, true)
      } else {
        graphSearch(x.tail, newTrace :+ y, false)
      }
    } else {
      backward(newTrace)
    }
  }

  def buildGrapgh(x: List[Row], y: List[Int]): (List[Int], Boolean) = {
    val z = List.fill(x.length)(0)

    z.zipWithIndex.foldLeft((List.empty[Int], true)) {
      (_, b) =>
        val (idx, i) = b
        val (newZ, fo) = findSide(x, y, z, idx)

        if (fo)
          (newZ, false)
        else
          (newZ, true)
    }
  }

  def findSide(x: List[Row], y: List[Int], z: List[Int], i: Int): (List[Int], Boolean) = {
    val z = i - 1 to 1 by -1 toList

    z.foldLeft((List.empty[Int], true)) {
      (acc, j) =>
        val (accZ, _) = acc

        val delay = x(i).get(6).toString.toInt //duration
        if (delay < 5 && x(i).get(3).toString.toInt == x(j).get(4).toString.toInt && delay > 0 && delay < 5) {
          if (y(j) > 0) {
            (accZ.updated(i, j), false)
          } else acc
        } else acc
    }
  }

  def backward(trace: List[List[Int]]): List[Int] = {
    val path = List.empty[Int]

    if (trace.isEmpty) {
      path :+ 1
    } else {
      val idx = trace.last.find(_ > 0)
      path :+ idx.get reverse //Unsafe.
    }
  }
}
