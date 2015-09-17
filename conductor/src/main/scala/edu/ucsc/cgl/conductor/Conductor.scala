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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class Config( s3PartSize: Int = 64 * 1024 * 1024,
                   hdfsBlockSize: Int = 64 * 1024 * 1024,
                   concat: Boolean = false,
                   src: URI,
                   dst: URI )

object Conductor
{
    def main( args: Array[String] )
    {
        val parser = new scopt.OptionParser[Config]( "scopt" )
        {
            arg[String]( "SRC_URL" ) required() action { ( x, c ) =>
                c.copy( src = new URI( x ) )
            } text "The URL to transfer from. Either s3://BUCKET/KEY or hdfs://HOST[:PORT]/PATH."

            arg[String]( "DST_URL" ) required() action { ( x, c ) =>
                c.copy( dst = new URI( x ) )
            } text "The URL to transfer to. Either hdfs://HOST[:PORT]/PATH or s3://BUCKET/KEY."

            // FIXME: Add examples to the help text.

            opt[Unit]( 'C', "concat" ) action { ( _, c ) =>
                c.copy( concat = true )
            } text "Concatenate all parts after the transfer."

            opt[Int]( 'p', "s3-part-size" ) action { ( x, c ) =>
                c.copy( s3PartSize = x * 1024 * 1024 )
            } text "The size of each S3 part in MB; default: 64"

            opt[Int]( 'b', "hdfs-block-size" ) action { ( x, c ) =>
                c.copy( hdfsBlockSize = x * 1024 * 1024 )
            } text "The block size in MB for files created on HDFS. The default is 64. Must " +
                "divide the S3 part size evenly. This option is only used when downloading to HDFS."
        }

        val credentials = Credentials( )

        val dummyUrl: URI = new URI( "" )
        java.lang.System.exit(
            parser.parse( args, Config( src = dummyUrl, dst = dummyUrl ) ) match {
                case Some( config ) =>
                    assert( config.s3PartSize % config.hdfsBlockSize == 0,
                        "S3 part size must be a multiple of HDFS block size." )
                    val (src, dst) = (config.src, config.dst)
                    val schemes = ("s3", "hdfs")
                    assert( List( schemes, schemes.swap ).contains( (src.getScheme, dst.getScheme) ),
                        "Unsupported combination of source and destination URL schemes. " +
                            "Expecting s3:// to hdfs:// or vice versa." )
                    val transfer = if( src.getScheme == "s3" ) {
                        new Download( config, credentials )
                    } else {
                        new Upload( config, credentials )
                    }
                    transfer.run( )
                    0
                case None =>
                    1
            } )
    }

    val blockSizeConfName = "edu.ucsc.cgl.conductor.Conductor.blockSize"
}

abstract class Transfer extends Serializable
{
    def sparkContext: SparkContext =
    {
        new SparkContext( new SparkConf( ).setAppName( this.getClass.getName ) )
    }

    def run( )
}

class Download( config: Config, credentials: Credentials ) extends Transfer
{
    assert( config.src.getScheme == "s3" )
    assert( config.dst.getScheme == "hdfs" )
    val srcBucket = config.src.getHost
    val srcPath = config.src.getPath
    assert( srcPath( 0 ) == '/', "The path to the source file must be valid and start with '/'." )
    val srcKey = srcPath.substring( 1 )
    val splitDst = new URI( config.dst.toString + ".parts" )

    def run( )
    {
        val s3 = new AmazonS3Client( credentials.toAwsCredentials )
        val objects =
            s3.listObjects( srcBucket, srcKey + "/" ).getObjectSummaries
            .asScala
            .filter( !_.getKey.endsWith( "_SUCCESS" ) )
            .sortBy( _.getKey )
        val keys = objects.map( _.getKey )
        val isFile = objects.isEmpty
        val sc = sparkContext
        try {
            val destination = if( config.concat ) splitDst else config.dst
            if( isFile ) {
                val size = s3.getObjectMetadata( srcBucket, srcKey ).getContentLength
                val partitions = partition( size )
                // Inject block size such that BinaryOutputFormat can extract it
                sc.hadoopConfiguration.setInt( Conductor.blockSizeConfName, config.hdfsBlockSize )
                sc.parallelize( partitions, partitions.size )
                    .map( partition => (partition, downloadPart( partition )) )
                    .saveAsHadoopFile(
                        destination.toString,
                        classOf[Object],
                        classOf[Array[Byte]],
                        classOf[BinaryOutputFormat[Object]] )
            } else {
                if( config.concat ) {
                    val srcPartSize = objects.head.getSize
                    if( objects.init.exists( p =>
                        p.getSize != srcPartSize || p.getSize % config.hdfsBlockSize != 0 )
                    ) {
                        throw new AssertionError(
                            "For parts to be concatenated, the size of all but the last part " +
                                "must be equal and divisible by the HDFS block size." )
                    }
                }
                sc.parallelize( keys )
                    .zipWithIndex( )
                    .foreach { case (key, index) => downloadFile( key, index, destination ) }
            }
            if( config.concat ) {
                concat( sc )
            } else {
                createSuccessFile( sc )
            }
        } finally {
            sc.stop( )
        }
    }

    def partition( size: Long ): ArrayBuffer[Partition] =
    {
        val numPartitions = size / config.s3PartSize
        val remainder = (size % config.s3PartSize).toInt
        val partitions = new ArrayBuffer[Partition]
        for( i <- 0L until numPartitions ) {
            partitions += new Partition( i * config.s3PartSize, config.s3PartSize )
        }
        if( remainder > 0 ) {
            partitions += new Partition( size - remainder, remainder )
        }
        partitions
    }

    def downloadPart( partition: Partition ): Array[Byte] =
    {
        val size = partition.getSize
        val start = partition.getStart
        assert( size > 0, "Partitions cannot be empty." )
        assert( size <= config.s3PartSize, "No partition can exceed the specified part size." )
        val s3 = new AmazonS3Client( credentials.toAwsCredentials )
        val req = new GetObjectRequest( srcBucket, srcKey )
        req.setRange( start, start + size - 1 )
        val in = s3.getObject( req ).getObjectContent
        try {
            /*
             * Knowing the input size is obviously an advantage over blindly copying from a stream
             * of unknown size. IOW, I expect the result array to be allocated exactly once.
             * Currently I can only find this in commons.io. Guava has it, too, but doesn't expose
             * it publicly. Also, anything based on ByteArrayOutputStream will likely incur one
             * final redundant copy when the ByteArrayOutputStream.toByteArray method is called.
             * scalax.io calls Thread.sleep() in its copy methods which leads to immediate
             * disqualification. But it doesn't allow specifying a known size either AFAICT.
             */
            IOUtils.toByteArray( in, size )
        } finally {
            in.close( )
        }
    }

    def downloadFile( key: String, index: Long, destination: URI ): Unit =
    {
        val slaveS3 = new AmazonS3Client( credentials.toAwsCredentials )
        val file = slaveS3.getObject( srcBucket, key )
        var partName = key.split( "/" )( 1 )
        if( config.concat ) {
            val partDigit = 5
            val formattedNumber =
                index.toString.reverse.padTo( partDigit, '0' ).reverse
            partName = "part-" + formattedNumber
        }
        val content = IOUtils.toByteArray( file.getObjectContent )
        val hadoopConfig = new Configuration( )
        val hdfsRootURI = new URI( config.dst.getScheme, config.dst.getHost, "/", "" )
        val hdfs = FileSystem.get( hdfsRootURI, hadoopConfig )
        try {
            val filePath = new Path( destination.toString + "/" + partName )
            val stream = hdfs.create(
                filePath,
                true,
                hdfs.getConf.getInt( "io.file.buffer.size", 4096 ),
                hdfs.getDefaultReplication( filePath ),
                config.hdfsBlockSize
            )
            try {
                stream.write( content )
            } finally {
                stream.close( )
            }
        } finally {
            hdfs.close( )
        }
    }

    def concat( sc: SparkContext ): Unit =
    {
        val dfs = FileSystem.get( config.dst, sc.hadoopConfiguration )
            .asInstanceOf[DistributedFileSystem]
        val splitDstPath = new Path( splitDst )
        val dstPath = new Path( config.dst )
        val parts = dfs.listStatus( splitDstPath, new PathFilter
        {
            override def accept( path: Path ): Boolean = path.getName.startsWith( "part-" )
        } ).map( _.getPath ).sortBy( _.getName )
        val Array( firstPart, otherParts@_* ) = parts
        if( otherParts.nonEmpty ) {
            dfs.concat( firstPart, otherParts.toArray )
        }
        dfs.rename( firstPart, dstPath )
        dfs.delete( splitDstPath, true )
    }

    def createSuccessFile( sc: SparkContext ): Unit =
    {
        val dfs = FileSystem.get( config.dst, sc.hadoopConfiguration )
            .asInstanceOf[DistributedFileSystem]
        try {
            dfs.createNewFile( new Path( config.dst.toString + "/_SUCCESS" ) )
        } finally {
            dfs.close( )
        }
    }
}

class Upload( config: Config, credentials: Credentials ) extends Transfer
{
    assert( config.src.getScheme == "hdfs" )
    assert( config.dst.getScheme == "s3" )
    val dstBucket = config.dst.getHost
    val dstPath = config.dst.getPath
    assert( dstPath( 0 ) == '/', "The path to the destination file must start with '/'." )
    val dstKey = dstPath.substring( 1 )
    val maxPartNumber = 10000

    def run( )
    {
        if( config.concat ) {
            uploadParts( )
        } else {
            uploadObjects( )
        }
    }

    def uploadParts( )
    {
        val sc = sparkContext
        try {
            val indexedRDD =
                sc.hadoopFile[Partition, Array[Byte], BinaryInputFormat]( config.src.toString )
                    // FIXME: why is this filter necessary?
                    .filter { case (partition, data) => !data.isEmpty }
                    .zipWithIndex( )
            val s3 = new AmazonS3Client( credentials.toAwsCredentials )
            val request = new InitiateMultipartUploadRequest( dstBucket, dstKey )
            val uploadId = s3.initiateMultipartUpload( request ).getUploadId
            try {
                // PartETag objects aren't serializable so we need to substitute them with tuples
                val partsAsTuples = indexedRDD.map { case ((partition, block), partNum) =>
                    (uploadPart( uploadId, partition, block, partNum ).getETag, partNum)
                }.collect( )
                // Now recreate the parts as PartETag objects
                val parts = partsAsTuples
                    .map { case (etag, partNum) => new PartETag( partNum.toInt + 1, etag ) }
                    .toBuffer
                    .asJava
                // And finalize the upload
                s3.completeMultipartUpload( new CompleteMultipartUploadRequest( )
                    .withBucketName( dstBucket )
                    .withKey( dstKey )
                    .withUploadId( uploadId )
                    .withPartETags( parts ) )
            } catch {
                case e: Exception =>
                    val request = new AbortMultipartUploadRequest( dstBucket, dstKey, uploadId )
                    s3.abortMultipartUpload( request )
                    throw e
            }
        } finally {
            sc.stop( )
        }
    }

    def uploadPart( uploadId: String,
                    partition: Partition,
                    block: Array[Byte],
                    partNum: Long ): PartETag =
    {
        val s3 = new AmazonS3Client( credentials.toAwsCredentials )
        val stream = new ByteArrayInputStream( block )
        val response = s3.uploadPart( new UploadPartRequest( )
            .withBucketName( dstBucket )
            .withKey( dstKey )
            .withUploadId( uploadId )
            .withPartNumber( partNum.toInt + 1 )
            .withInputStream( stream )
            .withPartSize( block.length ) )
        response.getPartETag
    }

    def uploadObjects( )
    {
        val sc = sparkContext
        try {
            val indexedRDD =
                sc.hadoopFile[Partition, Array[Byte], BinaryInputFormat]( config.src.toString )
                    // FIXME: why is this filter necessary?
                    .filter { case (partition, data) => !data.isEmpty }
                    .zipWithIndex( )
            val s3 = new AmazonS3Client( credentials.toAwsCredentials )
            indexedRDD.foreach { case ((partition, block), part) =>
                uploadObject( partition, block, part.toInt + 1 )
            }
            val metadata = new ObjectMetadata( )
            metadata.setContentLength( 0 )
            val emptyStream = new ByteArrayInputStream( new Array[Byte]( 0 ) )
            s3.putObject( dstBucket, dstKey + "/_SUCCESS", emptyStream, metadata )
        } finally {
            sc.stop( )
        }
    }

    def uploadObject( partition: Partition, block: Array[Byte], partNum: Int ) =
    {
        val s3 = new AmazonS3Client( credentials.toAwsCredentials )
        val partDigit = 5
        val metadata = new ObjectMetadata( )
        metadata.setContentLength( block.length )
        val stream = new ByteArrayInputStream( block )
        val formattedNumber: String = partNum.toString.reverse.padTo( partDigit, '0' ).reverse
        s3.putObject( dstBucket, dstKey + "/part-" + formattedNumber, stream, metadata )
    }
}

class BinaryOutputFormat[K] extends FileOutputFormat[K, Array[Byte]]
{

    override def getRecordWriter( ignored: FileSystem,
                                  job: JobConf,
                                  name: String,
                                  progress: Progressable ): mapred.RecordWriter[K, Array[Byte]] =
    {
        val file: Path = FileOutputFormat.getTaskOutputPath( job, name )
        val fs: FileSystem = file.getFileSystem( job )
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
            fs.getConf.getInt( "io.file.buffer.size", 4096 ),
            fs.getDefaultReplication( file ),
            job.getInt( Conductor.blockSizeConfName, 0 ),
            progress )
        new mapred.RecordWriter[K, Array[Byte]]
        {
            override def write( key: K, value: Array[Byte] ): Unit = { fileOut.write( value ) }

            override def close( reporter: Reporter ): Unit = { fileOut.close( ) }
        }
    }
}

class BinaryInputFormat extends FileInputFormat[Partition, Array[Byte]]
{
    override def getRecordReader( inputSplit: InputSplit,
                                  jobConf: JobConf,
                                  reporter: Reporter ): RecordReader[Partition, Array[Byte]] =
    {
        val fileSplit = inputSplit.asInstanceOf[FileSplit]
        val path: Path = fileSplit.getPath
        val fs: FileSystem = path.getFileSystem( jobConf )
        val fileSize = fs.getFileStatus( path ).getLen
        val fileIn: FSDataInputStream = fs.open( path )
        fileIn.seek( fileSplit.getStart )

        new RecordReader[Partition, Array[Byte]]
        {
            var x = true

            override def next( k: Partition, v: Array[Byte] ): Boolean =
            {
                if( x ) {
                    IOUtils.read( fileIn, v, 0, fileSplit.getLength.toInt )
                    x = false
                    true
                } else {
                    x
                }
            }

            override def getProgress: Float = { fileIn.getPos.toFloat / fileSize.toFloat }

            override def getPos: Long = { fileIn.getPos }

            override def createKey( ): Partition =
            {
                new Partition( fileSplit.getStart, fileSplit.getLength.toInt )
            }

            override def createValue( ): Array[Byte] =
            {
                new Array[Byte]( fileSplit.getLength.toInt )
            }

            override def close( ): Unit = { fileIn.close( ) }
        }
    }
}

class Partition( start: Long, size: Int ) extends Serializable
{
    def getStart: Long = { start }

    def getSize: Int = { size }
}

abstract class Credentials
{
    def toAwsCredentials: AWSCredentials
}


object Credentials
{
    def apply( ) =
    {
        // If we can get configured credentials on the driver, use those on every
        // worker node. Otherwise fall back to instance profile credentials which
        // will be read from instance metadata on each node. Note that even if the
        // driver node has access to instance profile credentials, we wouldn't be
        // able to use them on worker nodes.
        try {
            new ExplicitCredentials( new ConfiguredCredentials( ).toAwsCredentials )
        } catch {
            case _: AmazonClientException => new ImplicitCredentials
        }
    }
}


@SerialVersionUID( 0L )
case class ExplicitCredentials( accessKeyId: String, secretKey: String )
    extends Credentials with Serializable
{
    def this( awsCredentials: AWSCredentials )
    {
        this( awsCredentials.getAWSAccessKeyId, awsCredentials.getAWSSecretKey )
    }

    def toAwsCredentials: AWSCredentials =
    {
        new BasicAWSCredentials( accessKeyId, secretKey )
    }
}


@SerialVersionUID( 0L )
case class ConfiguredCredentials( )
    extends Credentials with Serializable
{
    def toAwsCredentials: AWSCredentials =
    {
        new AWSCredentialsProviderChain(
            new EnvironmentVariableCredentialsProvider,
            new SystemPropertiesCredentialsProvider,
            new ProfileCredentialsProvider
        ).getCredentials
    }
}


@SerialVersionUID( 0L )
case class ImplicitCredentials( )
    extends Credentials with Serializable
{
    def toAwsCredentials: AWSCredentials =
    {
        new DefaultAWSCredentialsProviderChain( ).getCredentials
    }
}
