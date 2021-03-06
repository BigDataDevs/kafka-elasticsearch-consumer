[![Build Status](https://travis-ci.org/BigDataDevs/kafka-elasticsearch-consumer.svg?branch=master)](https://travis-ci.org/BigDataDevs/kafka-elasticsearch-consumer)
[ ![Download](https://api.bintray.com/packages/bigdatadevs/bigdatadevs-repo/kafka-elasticsearch-consumer/images/download.svg) ](https://bintray.com/bigdatadevs/bigdatadevs-repo/kafka-elasticsearch-consumer/_latestVersion)

# Welcome to the kafka-elasticsearch-standalone-consumer wiki!

NOTE: The 'master' branch is moving to the heavily refactored new V2.0 version.

For the previous version, V1.0, use the 'version-1.0' branch

## Architecture of the kafka-elasticsearch-standalone-consumer [indexer]

![](img/IndexerV2Design.jpg)


# Introduction

### **Kafka Standalone Consumer reads messages from Kafka in batches, processes/transforms/enriches individual messages and processes the batch into a specified destination. **

Currently, an example implemented destination is ElasticSearch. One batch of messages is composed of all messages retrieved during one poll() call in the Kafka consumers.

**Main features:**

* starting offset positions can be specified via configuration properties
* IConsumerWorker interface-based consumers allow you to :

** customize how offsets are exposed to other systems - like JMX/monitoring or external storage

** customize when offsets are committed to Kafka - allowing you to specify your own logic/rules for re-processing of potentially failed events/batchs

* IBatchMessageProcessor interface-based batch processors can be customized to process each batch of messages into any destination 


# How to use ? 

### Running via Gradle 

1. Download the code into a `$INDEXER_HOME` dir.

2. `$INDEXER_HOME`/src/main/resources/config/kafka-es-indexer.properties file - update all relevant properties as explained in the comments.

3. `$INDEXER_HOME`/src/main/resources/config/logback.xml - specify directory you want to store logs in: `<property name="LOG_DIR" value="/tmp"/>`. Adjust values of max sizes and number of log files as needed.

4. `$INDEXER_HOME`/src/main/resources/config/kafka-es-indexer-start-options.config - consumer start options can be configured here (Start from earliest, latest, etc), more details inside a file.

5. modify `$INDEXER_HOME`/src/main/resources/spring/kafka-es-context-public.xml if needed

	If you want to use custom IBatchMessageProcessor class - specify it in the following config:
	(make sure to only modify the class name, not the beans' name/scope)
	
	`<bean id="messageProcessor"
          class="org.elasticsearch.kafka.indexer.service.impl.examples.ESBatchMessageProcessorImpl"
          scope="prototype"
        p:elasticSearchBatchService-ref="elasticSearchBatchService"/>`

6. build the app:

    `cd $INDEXER_HOME`

    `./gradlew clean jar`

    The **kafka-elasticsearch-consumer-0.0.2.0.jar** will be created in the `$INDEXER_HOME/build/libs/` dir.

7. make sure your `$JAVA_HOME` env variable is set (use JDK1.8 or above);
	you may want to adjust JVM options and other values in the `gradlew` script and `gradle.properties` file

8. run the app:

	`./gradlew run -Dindexer.properties=$INDEXER_HOME/src/main/resources/config/kafka-es-indexer.properties -Dlogback.configurationFile=$INDEXER_HOME/src/main/resources/config/logback.xml`
 
### Running via generated scripts:

* Steps 1 - 6 are the same
* run: `./gradlew clean installDist`

* `cd ./build/install/kafka-elasticsearch-consumer/bin` dir:
![](img/build-dir.png)

* run `export KAFKA_ELASTICSEARCH_CONSUMER_OPTS="-Dindexer.properties=$INDEXER_HOME/src/main/resources/config/kafka-es-indexer.properties -Dlogback.configurationFile=$INDEXER_HOME/src/main/resources/config/logback.xml"` or modify `./kafka-elasticsearch-consumer` script to include `KAFKA_ELASTICSEARCH_CONSUMER_OPTS` variable
* run `./kafka-elasticsearch-consumer` script

# Versions

* Kafka Version: 2.1.x

* ElasticSearch: 6.2.x

* JDK 1.8

# Configuration

Indexer application properties are specified in the kafka-es-indexer.properties file - you have to adjust properties for your env:
[kafka-es-indexer.properties](src/main/resources/config/kafka-es-indexer.properties).
You can specify you own properties file via `-Dindexer.properties=/abs-path/your-kafka-es-indexer.properties`

Logging properties are specified in the logback.xml file - you have to adjust properties for your env:
[logback.xml](src/main/resources/config/logback.xml).
You can specify your own logback config file via `-Dlogback.configurationFile=/abs-path/your-logback.xml` property

Indexer application Spring configuration is specified in the kafka-es-context-public.xml:
[kafka-es-context.xml](src/main/resources/spring/kafka-es-context-public.xml)

Consumer start options can be specified with system property `consumer.start.option`. The value of this property can be `RESTART`, `EARLIEST`, `LATEST`  which applied for all partitions or `CUSTOM` which requires additional property `consumer.custom.start.options.file`. The value of `consumer.custom.start.options.file` property is an absolute path to the custom start offsets configuration file. (Look to [kafka-es-indexer-custom-start-options.properties](src/main/resources/config/kafka-es-indexer-custom-start-options.properties)).
By default `RESTART` option is used for all partitions.

Examples:
- `-Dconsumer.start.option=RESTART`
- `-Dconsumer.start.option=LATEST`
- `-Dconsumer.start.option=EARLIEST`
- `-Dconsumer.start.option=CUSTOM -Dconsumer.custom.start.options.file=/abs-path/your-kafka-es-indexer-custom-start-options.properties`

# Customization

Indexer application can be easily customized. The main areas for customizations are: 
* message handling/conversion
	examples of use cases for this customization:
	- your incoming messages are not in a JSON format compatible with the expected ES message formats
	- your messages have to be enreached with data from other sources (via other meta-data lookups, etc.)
	- you want to selectively index messages into ES based on some custom criteria
* index name/type customization

## batch message processing customization 
Processing of each batch of messages, retrieved from Kafka in one poll() request, can be customized.
This can be done by implementing the IBatchMessageProcessor interface :

* `org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor` is an interface that defines main methods for reading events from Kafka, processing them, and bulk-intexing into ElasticSearch. One can implement all or some of the methods if custom behavior is needed. You can customize:
* `processMessage(...)` method to parse, enrich or transform an event from one format into another; 
** this is also where you could customize index names/ routing values 
** when adding events to ES batches, if needed - as this method would call 
**  `elasticSearchBatchService.addEventToBulkRequest(inputMessage, indexName, indexType, eventUUID, routingValue)` method
* `beforeCommitCallBack(...)` method - this is where you can customize at which point offsets are stored into Kafka; by default - it returns TRUE on each call, meaning that offsets will be committed after one poll is successfully processed; you can have a different logic for committing/storing offsets if needed
* there are more methods that could be customized - read the JavaDoc for the `org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor` interface 

To do this customization, you can implement the IBatchMessageProcessor interface and inject the `ElasticSearchBatchService` into your implementation class and delegate most of the methods to the ElasticSearchBatchService class. ElasticSearchBatchService gives you basic batching operations.

See `org.elasticsearch.kafka.indexer.service.impl.examples.ESMessageMessageProcessorImpl` for an example of such customization. 

* _**Don't forget to specify your custom message processor class in the kafka-es-context-public.xml file. By default, ESMessageMessageProcessorImpl will be used**_

## ES index name/type management customization 
Index name and index type management/determination customization can be done by providing custom logic in your implementation of the IBatchMessageProcessor interface:

* `org.elasticsearch.kafka.indexer.service.impl.examples.ESMessageMessageProcessorImpl` uses `elasticsearch.index.name` and `elasticsearch.index.type` values as configured in the kafka-es-indexer.properties file. If you want to use a different custom logic - do it in the `processMessage(...)` method

# Running as a Docker Container

TODO

# License

kafka-elasticsearch-standalone-consumer

	Licensed under the Apache License, Version 2.0 (the "License"); you may
	not use this file except in compliance with the License. You may obtain
	a copy of the License at

	     http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing,
	software distributed under the License is distributed on an
	"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	KIND, either express or implied.  See the License for the
	specific language governing permissions and limitations
	under the License.

# Contributors

 - [Krishna Raj](https://github.com/reachkrishnaraj)
 - [Marina Popova](https://github.com/ppine7)
 - [Dhyan Muralidharan](https://github.com/dhyan-yottaa)
 - [Andriy Pyshchyk](https://github.com/apysh)
 - [Vitalii Chernyak](https://github.com/merlin-zaraza)
