package edu.ucsc.bd2k

import java.net.URI

import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, Reporter}
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object SparkS3Downloader {

  val partitionSize = 64 * 1024 * 1024 // TODO: make configurable

  def main(args: Array[String]) {
    val src = new URI(args(0))
    assert(src.getScheme == "s3")
    val dst = new URI(args(1))
    assert(dst.getScheme == "hdfs")
    val credentials = new Credentials()
    new SparkS3Downloader(credentials, partitionSize, src, dst).run()
  }
}


class SparkS3Downloader(credentials: Credentials, partitionSize: Int, src: URI, dst: URI) extends Serializable {

  val srcBucket = src.getHost
  val srcPath = src.getPath
  assert(srcPath(0) == '/')
  val srcKey = srcPath.substring(1)
  val splitDst = new URI(dst.toString + ".parts")

  def run() {
    val s3 = new AmazonS3Client(credentials.toAwsCredentials)
    val size: Long = s3.getObjectMetadata(srcBucket, srcKey).getContentLength
    val partitions: ArrayBuffer[(Long, Int)] = partition(size)
    val conf = new SparkConf().setAppName("SparkS3Downloader")
    val sc = new SparkContext(conf)
    sc.parallelize(partitions, partitions.size)
      .map(partition => (partition, download(partition._1, partition._2)))
      .saveAsHadoopFile(splitDst.toString, classOf[Object], classOf[Array[Byte]], classOf[BinaryOutputFormat[Object]])
    concat(sc, splitDst, dst)
  }

  def partition(size: Long): ArrayBuffer[(Long, Int)] = {
    val numPartitions = size / partitionSize
    val remainder = (size % partitionSize).toInt
    val partitions = new ArrayBuffer[(Long, Int)]
    for (i <- 0L until numPartitions) {
      partitions += ((i * partitionSize, partitionSize))
    }
    if (remainder > 0) {
      partitions += ((size - remainder, remainder))
    }
    partitions
  }

  def download(start: Long, size: Int): Array[Byte] = {
    assert(size > 0)
    assert(size <= partitionSize)
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

  def concat(sc: SparkContext, splitDst: URI, dst: URI): Unit = {
    val dfs = FileSystem.get(dst, sc.hadoopConfiguration).asInstanceOf[DistributedFileSystem]
    val splitDstPath = new Path(splitDst)
    val dstPath = new Path(dst)
    val parts = dfs.listStatus(splitDstPath, new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith("part-")
    }).map(_.getPath).sortBy(_.getName)
    val Array(firstPart, otherParts @ _*) = parts
    if (otherParts.size > 0) {
      dfs.concat(firstPart, otherParts.toArray)
    }
    dfs.rename(firstPart, dstPath)
    dfs.delete(splitDstPath, true)
  }
}


class BinaryOutputFormat[K] extends FileOutputFormat[K, Array[Byte]] {
  override def getRecordWriter(ignored: FileSystem,
                               job: JobConf,
                               name: String,
                               progress: Progressable): mapred.RecordWriter[K, Array[Byte]] = {
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
      SparkS3Downloader.partitionSize,
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


@SerialVersionUID(0L)
class Credentials(val accessKeyId: String, val secretKey: String) extends Serializable {
  def this(awsCredentials: AWSCredentials) {
    this(awsCredentials.getAWSAccessKeyId, awsCredentials.getAWSSecretKey)
  }

  def this() {
    this(new DefaultAWSCredentialsProviderChain().getCredentials)
  }

  def toAwsCredentials: AWSCredentials = {
    new BasicAWSCredentials(accessKeyId, secretKey)
  }
}
