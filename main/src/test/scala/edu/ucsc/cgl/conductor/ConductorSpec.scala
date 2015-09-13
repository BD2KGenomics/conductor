/*
Copyright 2015 UCSC Computational Genomics Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package edu.ucsc.cgl.conductor

import java.lang.AssertionError
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

  "Downloader objects" should "have a source URI in the S3 filesystem" in {
    a [AssertionError] should be thrownBy {
      new Downloader(
        credentials,
        partSize,
        partSize,
        new URI(dst),
        new URI(dst),
        true)
    }
  }

  it should "have a destination URI in the HDFS filesystem" in {
    a [AssertionError] should be thrownBy {
      new Downloader(
        credentials,
        partSize,
        partSize,
        new URI(src),
        new URI(src),
        true)
    }
  }

  "Uploader objects" should "have a source URI in the HDFS filesystem" in {
    a [AssertionError] should be thrownBy {
      new Uploader(
        credentials,
        new URI(src),
        new URI(src),
        true)
    }
  }

  it should "have a destination URI in the S3 filesystem" in {
    a [AssertionError] should be thrownBy {
      new Uploader(
        credentials,
        new URI(dst),
        new URI(dst),
        true)
    }
  }
}