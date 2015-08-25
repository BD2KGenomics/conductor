package edu.ucsc.bd2k

import java.net.URI

import collection.mutable.Stack
import org.scalatest._

class ConductorSpec extends FlatSpec with Matchers {

  val credentials = Credentials()
  val partSize = 64 * 1024 * 1024
  val bigFileSize = 64 * 1024 * 1024 * 5 / 2
  val src = "s3://file/src"
  val dst = "hdfs://file/dst"
  var downloader =
    new Downloader(
      credentials,
      partSize,
      partSize,
      new URI(src),
      new URI(dst),
      true)

  "The partition method" should "divide a file into pieces corresponding to" +
    " the specified size" in {
    val partitionResult = downloader.partition(partSize).toArray
    assert(partitionResult.length == 1)
    assert(partitionResult(0).getSize == partSize)
    assert(partitionResult(0).getStart == 0)

    val minusResult = downloader.partition(partSize - 1).toArray
    assert(minusResult.length == 1)
    assert(minusResult(0).getSize == partSize - 1)
    assert(minusResult(0).getStart == 0)

    val plusResult = downloader.partition(partSize + 1).toArray
    assert(plusResult.length == 2)
    assert(plusResult(0).getSize == partSize)
    assert(plusResult(0).getStart == 0)
    assert(plusResult(1).getSize == 1)
    assert(plusResult(1).getStart == partSize)

    val oneResult = downloader.partition(1).toArray
    assert(oneResult.length == 1)
    assert(oneResult(0).getSize == 1)
    assert(oneResult(0).getStart == 0)

    val bigResult = downloader.partition(bigFileSize).toArray
    assert(bigResult.length == 3)
    assert(bigResult(0).getSize == partSize)
    assert(bigResult(0).getStart == 0)
    assert(bigResult(1).getSize == partSize)
    assert(bigResult(1).getStart == partSize)
    assert(bigResult(2).getSize == partSize / 2)
    assert(bigResult(2).getStart == partSize * 2)
  }
}