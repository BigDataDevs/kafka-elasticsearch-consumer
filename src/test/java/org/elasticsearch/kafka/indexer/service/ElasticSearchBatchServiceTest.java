package org.elasticsearch.kafka.indexer.service;

import static org.junit.Assert.fail;

import java.util.Iterator;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ElasticSearchBatchServiceTest {

	@Mock
	private ElasticSearchClientService elasticSearchClientService;

	private ElasticSearchBatchService elasticSearchBatchService = new ElasticSearchBatchService();

	private BulkRequestBuilder mockedBulkRequestBuilder = Mockito.mock(BulkRequestBuilder.class);
	private IndexRequestBuilder mockedIndexRequestBuilder = Mockito.mock(IndexRequestBuilder.class);
	private ListenableActionFuture<BulkResponse> mockedActionFuture = Mockito.mock(ListenableActionFuture.class);
	private BulkResponse mockedBulkResponse = Mockito.mock(BulkResponse.class);
	private String testIndexName = "unitTestsIndex";
	private String testIndexType = "unitTestsType";

	/**
	 * @throws java.lang.Exception
	 */

	@Before
	public void setUp() throws Exception {
		// Mock all required ES classes and methods
		elasticSearchBatchService.setElasticSearchClientService(elasticSearchClientService);
		Mockito.when(elasticSearchClientService.prepareBulk()).thenReturn(mockedBulkRequestBuilder);
		Mockito.when(elasticSearchClientService.prepareIndex(Matchers.anyString(), Matchers.anyString()))
				.thenReturn(mockedIndexRequestBuilder);
		Mockito.when(elasticSearchClientService.prepareIndex(Matchers.anyString(), Matchers.anyString(),
				Matchers.anyString())).thenReturn(mockedIndexRequestBuilder);
		Mockito.when(mockedIndexRequestBuilder.setSource(Matchers.anyString())).thenReturn(mockedIndexRequestBuilder);
		Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
		Mockito.when(mockedActionFuture.actionGet()).thenReturn(mockedBulkResponse);
		// mock the number of messages in the bulk index request to be 1
		Mockito.when(mockedBulkRequestBuilder.numberOfActions()).thenReturn(1);

		// Mockito.when(elasticIndexHandler.getIndexName(null)).thenReturn(testIndexName);
		// Mockito.when(elasticIndexHandler.getIndexType(null)).thenReturn(testIndexType);
	}

	@Test
	public void testAddEventToBulkRequest_withUUID_withRouting() {
		String message = "test message";
		String eventUUID = "eventUUID";
		// boolean needsRouting = true;

		try {
			elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
					eventUUID);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		
		
		// verify we called the prepareIndex with eventUUID
		Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndex(testIndexName, testIndexType,
				eventUUID);
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setSource(message, XContentType.JSON);
		// verify that routing is set to use eventUUID
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setRouting(eventUUID);
	}

	/**
	 * Test method for
	 * {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#postToElasticSearch()}
	 * .
	 */

	@Test
	public void testPostOneBulkRequestToES_NoNodeException() {
		// simulate failure due to ES cluster (or part of it) not being
		// available
		String message = "test message";
		String eventUUID = "eventUUID";
		// boolean needsRouting = true;

		try {
			elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
					eventUUID);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		
		Mockito.when(mockedBulkRequestBuilder.execute()).thenThrow(new NoNodeAvailableException("Unit Test Exception"));
		try {
			elasticSearchBatchService.postToElasticSearch();
		} catch (InterruptedException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		} catch (IndexerESRecoverableException e) {
			System.out.println(
					"Cought expected NoNodeAvailableException/IndexerESException from unit test: " + e.getMessage());
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

		Mockito.verify(mockedBulkRequestBuilder, Mockito.times(1)).execute();
		// verify that reInitElasticSearch() was called
		try {
			Mockito.verify(elasticSearchClientService, Mockito.times(1)).reInitElasticSearch();
		} catch (InterruptedException | IndexerESNotRecoverableException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
	}

	@Test
	public void testPostOneBulkRequestToES_postFailures() {
		
		String message = "test message";
		String eventUUID = "eventUUID";
		// boolean needsRouting = true;

		try {
			elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
					eventUUID);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		// make sure index request itself does not fail
		Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
		// mock failures from ES indexing
		Mockito.when(mockedBulkResponse.hasFailures()).thenReturn(true);
		BulkItemResponse bulkItemResponse = Mockito.mock(BulkItemResponse.class);
		Iterator<BulkItemResponse> bulkRespItr = Mockito.mock(Iterator.class);
		Mockito.when(bulkRespItr.hasNext()).thenReturn(true, false);
		Mockito.when(bulkRespItr.next()).thenReturn(bulkItemResponse);
		Mockito.when(mockedBulkResponse.iterator()).thenReturn(bulkRespItr);
		Mockito.when(bulkItemResponse.isFailed()).thenReturn(true);

		// mock the returned failure results
		Failure mockedFailure = Mockito.mock(Failure.class);
		Mockito.when(bulkItemResponse.getFailure()).thenReturn(mockedFailure);
		String failureMessage = "mocked bulkResponse failure message";
		Mockito.when(mockedFailure.getMessage()).thenReturn(failureMessage);
		RestStatus mockRestStatus = RestStatus.BAD_REQUEST;
		Mockito.when(mockedFailure.getStatus()).thenReturn(mockRestStatus);

		try {
			elasticSearchBatchService.postToElasticSearch();
		} catch (InterruptedException | IndexerESNotRecoverableException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Mockito.verify(mockedBulkRequestBuilder, Mockito.times(1)).execute();
		// verify that reInit was not called
		try {
			Mockito.verify(elasticSearchClientService, Mockito.times(0)).reInitElasticSearch();
		} catch (InterruptedException | IndexerESNotRecoverableException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

		// verify that getFailure() is called twice
		Mockito.verify(bulkItemResponse, Mockito.times(2)).getFailure();
	}

}
