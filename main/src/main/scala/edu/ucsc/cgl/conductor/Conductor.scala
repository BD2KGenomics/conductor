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

import java.io.ByteArrayInputStream
import java.net.URI

import com.amazonaws.AmazonClientException
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Conductor {

  def main(args: Array[String]) {   
    
    val credentials = Credentials()
    val parser = new scopt.OptionParser[Config]("scopt") {
      opt[Int]("s3-part-size") action { (x, c) =>
        c.copy(s3PartSize = x)} text("s3-part-size indicates the size of " +
        "each partition in MB; default: 64")
      opt[Int]("hdfs-block-size") action { (x, c) => 
        c.copy(hdfsBlockSize = x)} text("hdfs-block-size indicates the size" +
        " of each block in MB; must divide s3-part-size evenly; default: 64")
      opt[Unit]("concat") action { (_, c) =>
        c.copy(concat = true)} text("concatenates the parts of the files " +
        "after uploads and downloads")
      arg[String]("src-path") action { (x, c) =>
        c.copy(srcLocation = x) } text("location of src file; if " +
        "downloading, the src must be in s3; if uploading, the " +
        "src must be in hdfs")
      arg[String]("dst-path") action { (x, c) =>
        c.copy(dstLocation = x) } text("location of dst file; if " +
        "downloading, the dst must be in hdfs; if uploading, the " +
        "dst must be in s3")
    }
    parser.parse(args, Config()) match {
      case Some(config) =>
          val partitionSize = config.s3PartSize * 1024 * 1024
          val blockSize = config.hdfsBlockSize * 1024 * 1024
          assert(partitionSize % blockSize == 0,
            "Partition size must be a multiple of block size.")
          val src = new URI(config.srcLocation)
          assert(src.getScheme == "s3" || src.getScheme == "hdfs",
            "The source file must be in S3 or HDFS.")
          val dst = new URI(config.dstLocation)
          val concat = config.concat
          if (src.getScheme == "s3") {
            new Downloader(
              credentials,
              partitionSize,
              blockSize,
              src,
              dst,
              concat).run()
          } else if (src.getScheme == "hdfs") {
            new Uploader(credentials, src, dst, concat).run()
          }
      case None =>
        // arguments are bad, error message will have been displayed
    }
  }
}

case class Config(s3PartSize: Int = 64,
                  hdfsBlockSize: Int = 64,
                  srcLocation: String = "",
                  dstLocation: String = "",
                  test: Boolean = false,
                  concat: Boolean = false)

class Downloader(credentials: Credentials,
                 partitionSize: Int,
                 blockSize: Int,
                 src: URI,
                 dst: URI,
                 concat: Boolean) extends Serializable {

  assert(src.getScheme == "s3",
    "The source location for a download must be in S3.")
  assert(dst.getScheme == "hdfs",
    "The destination location for a download must be in HDFS" +
      " (Hadoop Distributed File System).")
  val srcBucket = src.getHost
  val srcPath = src.getPath
  assert(srcPath(0) == '/',
    "The path to the source file must be valid and start with '/'.")
  val srcKey = srcPath.substring(1)
  val splitDst = new URI(dst.toString + ".parts")
  val blockSizeConf = "sparkS3Downloader.blockSize"

  def run() {
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val objects = JavaConversions
      .asScalaBuffer(
        s3.listObjects(srcBucket, srcKey + "/").getObjectSummaries)
      .toList
      .filter(o => !o.getKey.endsWith("_SUCCESS"))
    val keys = objects.map(summary => summary.getKey)
    val isFile = objects.isEmpty
    val conf = new SparkConf().setAppName("SparkS3Downloader")
    val sc = new SparkContext(conf)
    try {
      var destination = dst
      if (concat) {
        destination = splitDst
      }
      if (isFile) {
        val size: Long =
          s3.getObjectMetadata(srcBucket, srcKey).getContentLength
        val partitions: ArrayBuffer[Partition] = partition(size)
        sc.hadoopConfiguration.setInt(blockSizeConf, blockSize)
        sc.parallelize(partitions, partitions.size)
          .map(partition => (partition, downloadPart(partition)))
          .saveAsHadoopFile(
            destination.toString,
            classOf[Object],
            classOf[Array[Byte]],
            classOf[BinaryOutputFormat[Object]])
      } else {
        val inputBlockSize = objects.head.getSize
        if (concat) {
          var validConcat = true
          for (n <- 0 to objects.length - 2) {
            val nSize = objects(n).getSize
            if (!(nSize == inputBlockSize && nSize % blockSize == 0)) {
              validConcat = false
            }
          }
          assert(validConcat,
            "For objects to be concatenated, all but the last one must be" +
              " the same size and be divisible by the HDFS block size.")
        }
        sc.makeRDD(keys)
          .zipWithIndex()
          .foreach(keyAndIndex =>
          downloadFile(keyAndIndex._1, keyAndIndex._2, destination))
      }
      if (concat) {
        concat(sc)
      } else {
        noConcat(sc)
      }
    } finally {
      sc.stop()
    }
  }

  def partition(size: Long): ArrayBuffer[Partition] = {
    val numPartitions = size / partitionSize
    val remainder = (size % partitionSize).toInt
    val partitions = new ArrayBuffer[Partition]
    for (i <- 0L until numPartitions) {
      partitions += new Partition(i * partitionSize, partitionSize)
    }
    if (remainder > 0) {
      partitions += new Partition(size - remainder, remainder)
    }
    partitions
  }

  def downloadPart(partition: Partition): Array[Byte] = {
    val size = partition.getSize
    val start = partition.getStart
    assert(size > 0, "Partitions cannot be empty.")
    assert(size <= partitionSize,
      "No partition can exceed the specified part size.")
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val req = new GetObjectRequest(srcBucket, srcKey)
    req.setRange(start, start + size - 1)
    val in = s3.getObject(req).getObjectContent
    try {
      /*
       * Knowing the input size is obviously an advantage over blindly copying
       * from a stream of unknown size. IOW, I expect the result array to be
       * allocated once. Currently I can only find this in commons.io. Guava
       * has it, too, but doesn't expose it publicly. Also, anything based on
       * ByteArrayOutputStream will likely incur one final redundant copy when
       * the ByteArrayOutputStream.toByteArray method is called. scalax.io
       * calls Thread.sleep() in its copy methods which leads to immediate
       * disqualification. But it also doesn't allow specifying a known size
       * from what I can tell.
       */
      IOUtils.toByteArray(in, size)
    } finally {
      in.close()
    }
  }

  def downloadFile(key: String, index: Long, destination: URI): Unit = {
    val slaveS3 = new AmazonS3Client(credentials.toAwsCredentials)
    val file = slaveS3.getObject(srcBucket, key)
    var partName = key.split("/")(1)
    if (concat) {
      val partDigit = 5
      val formattedNumber =
        index.toString.reverse.padTo(partDigit, '0').reverse
      partName = "part-" + formattedNumber
    }
    val content = IOUtils.toByteArray(file.getObjectContent)
    val config = new Configuration()
    val hdfsRootURI = new URI(dst.getScheme, dst.getHost, "/","")
    val hdfs = FileSystem.get(hdfsRootURI, config)
    try {
      val filePath = new Path(destination.toString + "/" + partName)
      val stream = hdfs.create(
        filePath,
        true,
        hdfs.getConf.getInt("io.file.buffer.size", 4096),
        hdfs.getDefaultReplication(filePath),
        blockSize
      )
      try {
      stream.write(content)
      } finally {
        stream.close()
      }
    } finally {
      hdfs.close()
    }
  }

  def concat(sc: SparkContext): Unit = {
    val dfs =
      FileSystem.get(dst, sc.hadoopConfiguration)
        .asInstanceOf[DistributedFileSystem]
    val splitDstPath = new Path(splitDst)
    val dstPath = new Path(dst)
    val parts = dfs.listStatus(splitDstPath, new PathFilter {
      override def accept(path: Path): Boolean =
        path.getName.startsWith("part-")
    }).map(_.getPath).sortBy(_.getName)
    val Array(firstPart, otherParts @ _*) = parts
    if (otherParts.nonEmpty) {
      dfs.concat(firstPart, otherParts.toArray)
    }
    dfs.rename(firstPart, dstPath)
    dfs.delete(splitDstPath, true)
  }

  def noConcat(sc: SparkContext): Unit = {
    val hdfs =
      FileSystem.get(dst, sc.hadoopConfiguration)
        .asInstanceOf[DistributedFileSystem]
    try {
      hdfs.createNewFile(new Path(dst.toString + "/_SUCCESS"))
    } finally {
      hdfs.close()
    }
  }
}

class Uploader(credentials: Credentials,
               src: URI,
               dst: URI,
               concat: Boolean) extends java.io.Serializable {

  assert(src.getScheme == "hdfs",
    "The source location for an upload must be in HDFS.")
  assert(dst.getScheme == "s3",
    "The destination location for an upload must be in S3.")
  val dstBucket = dst.getHost
  val dstPath = dst.getPath
  assert(dstPath(0) == '/',
    "The path to the destination file must be valid and start with '/'.")
  val dstKey = dstPath.substring(1)
  val maxPartNumber = 10000

  def run() {
    if (concat) {
      concatUpload()
    } else {
      nonConcatUpload()
    }
  }

  def concatUpload() {
    val conf = new SparkConf().setAppName("SparkS3Uploader")
    val sc = new SparkContext(conf)
    try {
      val hadoopRDD =
        sc.hadoopFile[Partition, Array[Byte], BinaryInputFormat](src.toString)
      val indexedRDD = hadoopRDD
        .filter(pair => !pair._2.isEmpty)
        .zipWithIndex()
      val s3 = new AmazonS3Client(credentials.toAwsCredentials)
      val response =
        s3.initiateMultipartUpload(
          new InitiateMultipartUploadRequest(dstBucket, dstKey))
      val uploadId: String = response.getUploadId
      try {
        val eTagRDD = indexedRDD.map(Function.tupled(
          (partData: (Partition, Array[Byte]), part: Long) => {
            val partition = partData._1
            val block = partData._2
            val partETag =
              concatPartUpload(uploadId, partition, block, part.toInt + 1)
            (partETag.getETag, partETag.getPartNumber)
          }))
        val partETags: java.util.List[PartETag] = eTagRDD
          .collect()
          .map(Function.tupled((eTag: String, part: Int) =>
          new PartETag(part, eTag)))
          .toBuffer
          .asJava
        s3.completeMultipartUpload(new CompleteMultipartUploadRequest()
          .withBucketName(dstBucket)
          .withKey(dstKey)
          .withUploadId(uploadId)
          .withPartETags(partETags))
      } catch {
        case e: Exception =>
          s3.abortMultipartUpload(
            new AbortMultipartUploadRequest(dstBucket, dstKey, uploadId))
          e.printStackTrace()
      }
    } finally {
      sc.stop()
    }
  }

  def concatPartUpload(uploadId: String,
                       partition: Partition,
                       block: Array[Byte],
                       partNum: Int): PartETag = {
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val stream = new ByteArrayInputStream(block)
    val response = s3.uploadPart(new UploadPartRequest()
      .withBucketName(dstBucket)
      .withKey(dstKey)
      .withUploadId(uploadId)
      .withPartNumber(partNum.toInt)
      .withInputStream(stream)
      .withPartSize(block.length))
    response.getPartETag
  }

  def nonConcatUpload() {
    val conf = new SparkConf().setAppName("SparkS3Uploader")
    val sc = new SparkContext(conf)
    try {
      val hadoopRDD =
        sc.hadoopFile[Partition, Array[Byte], BinaryInputFormat](src.toString)
      val indexedRDD = hadoopRDD
        .filter(pair => !pair._2.isEmpty)
        .zipWithIndex()
      val s3 = new AmazonS3Client(credentials.toAwsCredentials)
      indexedRDD.foreach(Function.tupled(
        (partData: (Partition, Array[Byte]), part: Long) => {
          val partition = partData._1
          val block = partData._2
          nonConcatPartUpload(partition, block, part.toInt + 1)
        }))
      val metadata = new ObjectMetadata()
      metadata.setContentLength(0)
      s3.putObject(
        dstBucket,
        dstKey + "/_SUCCESS",
        new ByteArrayInputStream(new Array[Byte](0)),
        metadata)
    } finally {
      sc.stop()
    }
  }

  def nonConcatPartUpload(partition: Partition,
                          block: Array[Byte],
                          partNum: Int) = {
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val partDigit = 5
    val metadata = new ObjectMetadata()
    metadata.setContentLength(block.length)
    val stream = new ByteArrayInputStream(block)
    val formattedNumber: String =
      partNum.toString.reverse.padTo(partDigit, '0').reverse
    s3.putObject(
      dstBucket,
      dstKey + "/part-" + formattedNumber,
      stream,
      metadata)
  }

}


class BinaryOutputFormat[K] extends FileOutputFormat[K, Array[Byte]] {

  val blockSizeConf = "sparkS3Downloader.blockSize"

  override def getRecordWriter(ignored: FileSystem,
                               job: JobConf,
                               name: String,
                               progress: Progressable):
  mapred.RecordWriter[K, Array[Byte]] = {
    val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
    val fs: FileSystem = file.getFileSystem(job)
    /*
     * Concatenation only works if all but the last part use an integral
     * number of blocks. We ensure this by setting the block size for this
     * "file" (quotes because it really is a directory) to the partition
     * size. That way, each part except the last one will take up one block.
     */
    // FIXME: This call duplicates too much internal logic from FileSystem
    val fileOut: FSDataOutputStream = fs.create(
      file,
      true,
      fs.getConf.getInt("io.file.buffer.size", 4096),
      fs.getDefaultReplication(file),
      job.getInt(blockSizeConf, 0),
      progress)
    new mapred.RecordWriter[K, Array[Byte]] {
      override def write(key: K, value: Array[Byte]): Unit = {
        fileOut.write(value)
      }

      override def close(reporter: Reporter): Unit = {
        fileOut.close()
      }
    }
  }
}

class BinaryInputFormat extends FileInputFormat[Partition, Array[Byte]] {

  override def getRecordReader(inputSplit: InputSplit,
                               jobConf: JobConf,
                               reporter: Reporter):
  RecordReader[Partition, Array[Byte]] = {
    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    val path: Path = fileSplit.getPath
    val fs: FileSystem = path.getFileSystem(jobConf)
    val fileSize = fs.getFileStatus(path).getLen
    val fileIn: FSDataInputStream = fs.open(path)
    fileIn.seek(fileSplit.getStart)

    new RecordReader[Partition, Array[Byte]] {
      var x = true
      override def next(k: Partition, v: Array[Byte]): Boolean = {
        if (x) {
          IOUtils.read(fileIn, v, 0, fileSplit.getLength.toInt)
          x = false
          true
        } else {
          x
        }
      }

      override def getProgress: Float = {
        fileIn.getPos.toFloat / fileSize.toFloat
      }

      override def getPos: Long = {
        fileIn.getPos
      }

      override def createKey(): Partition = {
        new Partition(fileSplit.getStart, fileSplit.getLength.toInt)
      }

      override def close(): Unit = {
        fileIn.close()
      }

      override def createValue(): Array[Byte] = {
        new Array[Byte](fileSplit.getLength.toInt)
      }
    }
  }
}

class Partition(start: Long, size: Int) extends Serializable {
  def getStart: Long = {
    start
  }

  def getSize: Int = {
    size
  }
}

abstract class Credentials {
  def toAwsCredentials: AWSCredentials
}


object Credentials {
  def apply() = {
    // If we can get configured credentials on the driver, use those on every
    // worker node. Otherwise fall back to instance profile credentials which
    // will be read from instance metadata on each node. Note that even if the
    // driver node has access to instance profile credentials, we wouldn't be
    // able to use them on worker nodes.
    try {
      new ExplicitCredentials(new ConfiguredCredentials().toAwsCredentials)
    } catch {
      case _: AmazonClientException => new ImplicitCredentials
    }
  }
}


@SerialVersionUID(0L)
case class ExplicitCredentials(accessKeyId: String, secretKey: String)
  extends Credentials with Serializable {

  def this(awsCredentials: AWSCredentials) {
    this(awsCredentials.getAWSAccessKeyId, awsCredentials.getAWSSecretKey)
  }

  def toAwsCredentials: AWSCredentials = {
    new BasicAWSCredentials(accessKeyId, secretKey)
  }
}


@SerialVersionUID(0L)
case class ConfiguredCredentials()
  extends Credentials with Serializable {

  def toAwsCredentials: AWSCredentials = {
    new AWSCredentialsProviderChain(
      new EnvironmentVariableCredentialsProvider,
      new SystemPropertiesCredentialsProvider,
      new ProfileCredentialsProvider
    ).getCredentials
  }
}


@SerialVersionUID(0L)
case class ImplicitCredentials()
  extends Credentials with Serializable {

  def toAwsCredentials: AWSCredentials = {
    new DefaultAWSCredentialsProviderChain().getCredentials
  }
}
