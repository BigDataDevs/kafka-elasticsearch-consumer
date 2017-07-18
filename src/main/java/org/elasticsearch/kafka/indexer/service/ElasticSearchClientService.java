package org.elasticsearch.kafka.indexer.service;

import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Created by dhyan on 8/31/15.
 */
// TODO convert to a singleton Spring ES service when ready
@Service
public class ElasticSearchClientService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientService.class);
    public static final String CLUSTER_NAME = "cluster.name";

    @Value("${elasticsearch.cluster.name:elasticsearch}")
    private String esClusterName;
    @Value("#{'${elasticsearch.hosts.list:localhost:9300}'.split(',')}")
    private List<String> esHostPortList;
    // sleep time in ms between attempts to index data into ES again
    @Value("${elasticsearch.indexing.retry.sleep.ms:10000}")
    private   int esIndexingRetrySleepTimeMs;
    // number of times to try to index data into ES if ES cluster is not reachable
    @Value("${elasticsearch.indexing.retry.attempts:2}")
    private   int numberOfEsIndexingRetryAttempts;

    // TODO add when we can inject partition number into each bean
	//private int currentPartition;
	private TransportClient esTransportClient;

    @PostConstruct
    public void init() throws Exception {
    	logger.info("Initializing ElasticSearchClient ...");
        // connect to elasticsearch cluster
        Settings settings = Settings.builder().put(CLUSTER_NAME, esClusterName).build();
        try {
        	
        	//new PreBuiltTransportClient(
            esTransportClient  = new PreBuiltTransportClient(settings);
            for (String eachHostPort : esHostPortList) {
                logger.info("adding [{}] to TransportClient ... ", eachHostPort);
                String[] hostPortTokens = eachHostPort.split(":");
                if (hostPortTokens.length < 2) 
                	throw new Exception("ERROR: bad ElasticSearch host:port configuration - wrong format: " + 
                		eachHostPort);
                int port = 9300; // default ES port
                try {
                	port = Integer.parseInt(hostPortTokens[1].trim());
                } catch (Throwable e){
                	logger.error("ERROR parsing port from the ES config [{}]- using default port 9300", eachHostPort);
                }
                esTransportClient.addTransportAddress(new InetSocketTransportAddress(
                		new InetSocketAddress(hostPortTokens[0].trim(), port)));
            }
            logger.info("ElasticSearch Client created and intialized OK");
        } catch (Exception e) {
            logger.error("Exception trying to connect and create ElasticSearch Client: "+ e.getMessage());
            throw e;
        }
    }

	@PreDestroy
    public void cleanup() throws Exception {
		//logger.info("About to stop ES client for partition={} ...", currentPartition);
		logger.info("About to stop ES client ...");
		if (esTransportClient != null)
			esTransportClient.close();
    }
    
	public void reInitElasticSearch() throws InterruptedException, IndexerESNotRecoverableException {
		for (int i=1; i<=numberOfEsIndexingRetryAttempts; i++ ){
			Thread.sleep(esIndexingRetrySleepTimeMs);
			//logger.warn("Re-trying to connect to ES, partition {}, try# {}", currentPartition, i);
			logger.warn("Re-trying to connect to ES, try# {}", i);
			try {
				init();
				// we succeeded - get out of the loop
				return;
			} catch (Exception e) {
				if (i<numberOfEsIndexingRetryAttempts){
					//logger.warn("Re-trying to connect to ES, partition {}, try# {} - failed again: {}", 
					//		currentPartition, i, e.getMessage());						
					logger.warn("Re-trying to connect to ES, try# {} - failed again: {}", 
							i, e.getMessage());						
				} else {
					//we've exhausted the number of retries - throw a IndexerESException to stop the IndexerJob thread
					//logger.error("Re-trying connect to ES, partition {}, "
					//		+ "try# {} - failed after the last retry; Will keep retrying ", currentPartition, i);						
					logger.error("Re-trying connect to ES, try# {} - failed after the last retry", i);						
					//throw new IndexerESException("ERROR: failed to connect to ES after max number of retiries, partition: " +
					//		currentPartition);
					throw new IndexerESNotRecoverableException("ERROR: failed to connect to ES after max number of retries ");
				}
			}
		}
	}

	public void deleteIndex(String index) {
		esTransportClient.admin().indices().prepareDelete(index).execute().actionGet();
		logger.info("Delete index {} successfully", index);
	}

	public void createIndex(String indexName){
		esTransportClient.admin().indices().prepareCreate(indexName).execute().actionGet();
		logger.info("Created index {} successfully",  indexName);
	}

	public void createIndexAndAlias(String indexName,String aliasName){
		esTransportClient.admin().indices().prepareCreate(indexName).addAlias(new Alias(aliasName)).execute().actionGet();
		logger.info("Created index {} with alias {} successfully" ,indexName,aliasName);
	}

	public void addAliasToExistingIndex(String indexName, String aliasName) {
		esTransportClient.admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
		logger.info("Added alias {} to index {} successfully" ,aliasName,indexName);
	}
	
	public void addAliasWithRoutingToExistingIndex(String indexName, String aliasName, String field, String fieldValue) {
		esTransportClient.admin().indices().prepareAliases().addAlias(indexName, aliasName, QueryBuilders.termQuery(field, fieldValue)).execute().actionGet();
		logger.info("Added alias {} to index {} successfully" ,aliasName,indexName);
	}

	public IndexRequestBuilder prepareIndex(String indexName, String indexType, String eventUUID) {
		return esTransportClient.prepareIndex(indexName, indexType, eventUUID);
	}

	public IndexRequestBuilder prepareIndex(String indexName, String indexType) {
		return esTransportClient.prepareIndex(indexName, indexType);
	}

	public BulkRequestBuilder prepareBulk() {
		return esTransportClient.prepareBulk();
	}

	public TransportClient getEsTransportClient() {
		return esTransportClient;
	}

}
