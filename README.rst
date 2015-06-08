Efficient, distributed downloads of large files from S3 to HDFS using Spark.

Hadoop's distcp utility supports downloading from S3 but does not distribute
the download of a single large file over multiple nodes. Amazon's s3distcp is
intended to fill that gap but, to our best knowledge, hasn't not been
released as open source.

A cluster of 10 r3.xlarge nodes downloaded a 288GB file in 377s to an HDFS
with replication factor 1. That's 783 MB/s. distcp typically gives you 50MB/s
to 80MB/s on that instance type. A cluster of 100 r3.xlarge nodes downloaded
that same file in 80s.

Prerequisites
=============

Run time:

* JRE 1.7+
* HDFS cluster
* Spark cluster

Build time:

* JDK 1.7+
* Scala SDK 2.10
* Maven

Scala 2.11 and Java 1.8 may work, too. We simply haven't tested those, yet.

Usage
=====

::

    export AWS_ACCESS_KEY=...
    export AWS_SECRET_KEY=...
    spark-submit spark-s3-downloader-VERSION.jar s3://BUCKET/KEY hdfs://HOST[:PORT]/PATH

Build
=====

::

    mvn package

Caveats
=======

* Alpha-quality
* Uses Spark, not Yarn/MapReduce
* Lack of unit tests
* Hard-coded task and block size of 64MB
* Destination must be a full ``hdfs://`` URL, the ``fs.default.name``
  property is ignored
* On failure, temporary files may be left around
* S3 credentials may be set via Java properties or environment variables as
  described in the `AWS API documentation`_ but are not read from
  ``core-site.xml``

.. _`AWS API documentation`: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
