# CassandradumpJ

## Description

This is a port of https://github.com/gianlucaborello/cassandradump - in Java.

Quoted verbatim from the original GitHub:

A data exporting tool for Cassandra inspired from mysqldump, with some additional slice and dice capabilities.

Disclaimer: most of the times, you really shouldn't be using this. It's fragile, non-scalable, inefficient and verbose. Cassandra already offers excellent exporting/importing tools:

 - Snapshots
 - CQL's COPY FROM/TO
 - sstable2json

However, especially during development, I frequently need to:

 - Quickly take a snapshot of an entire keyspace, and import it just as quickly without copying too many files around or losing too much time
 - Ability to take a very small subset of a massive production database (according to some CQL-like filtering) and import it quickly on my development environment

If these use cases sound familiar, this tool might be useful for you.

It's still missing many major Cassandra features that I don't use daily, so feel free to open an issue pointing them out (or send a pull request) if you need something.

## Requirements

To run this software, you must have at least a Java JRE version 1.8. You can check your Java version by running `java -version`

To build this software, you will need a JDK version 1.8, as well as Apache Maven and an internet connection. I am building this, at time of writing, with Apache Maven version 3.8.7

## Usage
At its most basic, run 

`java -jar cassandradumpJ-*-jar-with-dependencies.jar`

If you append a `-h` to the command, the help information will be printed, showing all of the various arguments, which should match the original Python.

## Building

To build the software, clone this repository, open a terminal inside the cloned directory, and run `mvn clean install`

The resultant build will be located in `./target` as per standard Maven practices.

The source file (presently there is only one) is found in `src/main/java`

## Testing

This software has a very basic framework for testing using JUnit.

The test does execute a sort of embedded Apache Cassandra instance, designed for the purpose of Unit Testing - I believe it will attempt to bind on the standard Cassandra ports, and may not work if you execute these tests on a system that is already running Apache Cassandra.

The tests are run automatically when the software is built - to run them explicitly, execute `mvn test`

The source for the Test file is found in `src/test/java`

## Current Issues/Limitations
The port is not 100% complete. To be frank, my focus was on getting the functionality I needed most ported over, though I did my best to port over any low-hanging features, some of which have been implemented but not tested.

- When specifying `--protocol-version` (which is needed for Authentication), version 1 is not supported because I'm not sure how to set up the credentials
- `--ssl` based Authentication is not supported because I don't personally use it and getting it set up in Java proved to be...non-trivial. The presumed process/framework for completing the feature is in place, however.
- `--username` and `--password` authentication should work but is untested
- When `--import-file` is specified, `--sync` must also be specified because the DataStax Java Driver does not have any sort of syntax candy for handling concurrent import statements (which the Python version did) - so, for that to work, a whole concurrency package (e.g. a Thread Pool/ExecutorService) would have to be introduced - I frankly do not use the `import` feature, so I did not implement this - however, the JUnit test does test the (synchronous) import, and it does work in that specific case.

Beyond that, a large portion of the parameters have never been tested, as I have no personal use case for them. I ported them by eye as best I could and they _probably_ work, but I wouldn't sign my name on them. These include:
 - `--cf`
 - `--filter`
 - `--host`
 - `--port`
 - `--exclude-cf`
 - `--no-create`
 - `--no-insert`
 - `--quiet`
 - `--limit`

However, to the best of my knowledge, all of these parameters have been implemented correctly. If you intend to use any of them, I'd encourage you to write Unit tests for them to validate their function before attempting to use them, particularly in any valuable/sensitive environments.

## TODOs
 - Add the built JAR file to the GitHub Releases