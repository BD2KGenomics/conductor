package edu.ucsc.bd2k

import java.io.FileOutputStream
import java.net.URI
import java.util

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CompleteMultipartUploadRequest, ListMultipartUploadsRequest, InitiateMultipartUploadRequest}
import org.apache.spark.{SparkContext, SparkConf}

import java.io.File
import java.util.Arrays

import scala.util.Random

object Tests {
  var sc: SparkContext = null
  var s3: AmazonS3Client = null
  val bucketName = "s3-downloader-tests"
  val partSize = 64 * 1024 * 1024
  val bigFileSize = 64 * 1024 * 1024 * 5 / 2 // A file to be divided into 2 full partitions and one half paritition
  val simple = "simple_file"
  val big = "big_file"
  var simpleBytes: Array[Byte] = null
  var simpleSs3d: SparkS3Downloader = null
  var bigBytes: Array[Byte] = null
  var bigSs3d: SparkS3Downloader = null
  var ss3u: SparkS3Uploader = null
  val uploadName = "upload_test"

  def start(credentials: Credentials): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("spark-s3-downloader Tests")
    sc = new SparkContext(conf)
    s3 = new AmazonS3Client(credentials.toAwsCredentials)
    s3.createBucket(bucketName)

    simpleBytes = uploadFile(partSize, simple)
    val simpleSrc = "s3://" + bucketName + "/" + simple
    val simpleDst = "hdfs://" + simple
    simpleSs3d = new SparkS3Downloader(credentials, partSize, partSize, new URI(simpleSrc), new URI(simpleDst), true)

    bigBytes = uploadFile(bigFileSize, big)
    val bigSrc = "s3://" + bucketName + "/" + big
    val bigDst = "hdfs://" + big
    bigSs3d = new SparkS3Downloader(credentials, partSize, partSize, new URI(bigSrc), new URI(bigDst), true)

    val ss3u = new SparkS3Uploader(credentials, new URI(bigDst), new URI(bigSrc), true)
  }

  def uploadFile(size: Int, name: String): Array[Byte] = {
    val dst = name
    val bytes = new Array[Byte](size)
    val random = new Random()
    random.nextBytes(bytes)
    val fos = new FileOutputStream(new File(dst))
    fos.write(bytes)
    s3.putObject(bucketName, name, new java.io.File(dst))
    bytes
  }

  def run(): Unit = {
    partitionTest()
    downloadTest()
    //sparkS3DownloaderTest()
    //uploadTest
  }

  /*
  Verifies that all partitions are assigned as they should be expected to be.
  Uses sizes of the partition size, partition size - 1, partition size + 1, and 1.
   */
  def partitionTest(): Unit = {
    val partitionResult = simpleSs3d.partition(partSize).toArray
    assert(partitionResult.length == 1)
    assert(partitionResult(0).getSize == partSize)
    assert(partitionResult(0).getStart == 0)

    val minusResult = simpleSs3d.partition(partSize - 1).toArray
    assert(minusResult.length == 1)
    assert(minusResult(0).getSize == partSize - 1)
    assert(minusResult(0).getStart == 0)

    val plusResult = simpleSs3d.partition(partSize + 1).toArray
    assert(plusResult.length == 2)
    assert(plusResult(0).getSize == partSize)
    assert(plusResult(0).getStart == 0)
    assert(plusResult(1).getSize == 1)
    assert(plusResult(1).getStart == partSize)

    val oneResult = simpleSs3d.partition(1).toArray
    assert(oneResult.length == 1)
    assert(oneResult(0).getSize == 1)
    assert(oneResult(0).getStart == 0)

    val bigResult = bigSs3d.partition(bigFileSize).toArray
    assert(bigResult.length == 3)
    assert(bigResult(0).getSize == partSize)
    assert(bigResult(0).getStart == 0)
    assert(bigResult(1).getSize == partSize)
    assert(bigResult(1).getStart == partSize)
    assert(bigResult(2).getSize == partSize / 2)
    assert(bigResult(2).getStart == partSize * 2)
  }

  /*
  Verifies that the bytes contained in a partition are the same as the bytes
  created randomly in those positions of the file.
   */
  def downloadTest(): Unit = {
    val simpleResult = simpleSs3d.partition(partSize).toArray
    val simplePart = simpleResult(0)
    assert(simpleSs3d.downloadPart(simplePart).sameElements(simpleBytes))

    val bigResult = bigSs3d.partition(bigFileSize).toArray
    assert(bigSs3d.downloadPart(bigResult(0)).sameElements(Arrays.copyOfRange(bigBytes, 0, partSize)))
    assert(bigSs3d.downloadPart(bigResult(1)).sameElements(Arrays.copyOfRange(bigBytes, partSize, 2 * partSize)))
    assert(bigSs3d.downloadPart(bigResult(2)).sameElements(Arrays.copyOfRange(bigBytes, 2 * partSize, bigFileSize)))
  }

  def sparkS3DownloaderTest(): Unit = {
    sc.stop()
    simpleSs3d.run()
    bigSs3d.run()
  }

  /*def uploadTest(): Unit = {
    val uploadId = s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, uploadName)).getUploadId
    ss3u.upload(uploadId, new Partition(0, partSize), simpleBytes)
    s3.completeMultipartUpload(new CompleteMultipartUploadRequest())
  }*/

  def stop(): Unit = {
    deleteFile(simple)
    deleteFile(big)
    s3.deleteBucket(bucketName)
    sc.stop()
  }

  def deleteFile(name: String): Unit = {
    s3.deleteObject(bucketName, name)
  }
}
