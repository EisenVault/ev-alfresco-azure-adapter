package com.eisenvault.azure;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Map;

import org.alfresco.repo.content.AbstractContentStore;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.content.ContentStoreCreatedEvent;
import org.alfresco.repo.content.UnsupportedContentUrlException;
import org.alfresco.repo.content.filestore.FileContentStore;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.alfresco.util.GUID;
import org.alfresco.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;


//import com.amazonaws.services.s3.transfer.TransferManager;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class AzureContentStore extends AbstractContentStore implements
		ApplicationContextAware, ApplicationListener<ApplicationEvent> {

	private static final Log logger = LogFactory
			.getLog(AzureContentStore.class);
	private ApplicationContext applicationContext;

	//private TransferManager transferManager;

	private String accessKey;
	private String accountName;
	private String containerName;
	private String rootDirectory;
	//private String pathToObject;

	private CloudStorageAccount storageAccount;
	private CloudBlobClient blobClient;
	private CloudBlobContainer container;

	@Override
	public boolean isWriteSupported() {
		return true;
	}

	@Override
	public ContentReader getReader(String contentUrl) {

		String pathToObject = makePathToObject(contentUrl);
		return new AzureContentReader(pathToObject, contentUrl, container);

	}

	public void init() {
		try {
			// check if access key is specified
			if (StringUtils.isNotBlank(this.accessKey)) {

				logger.debug("Found Azure Access Key in properties file");

			} else {
				throw new Exception(
						"Azure Access Key not specified in properties");
			}
			// check if account name is specified
			if (StringUtils.isNotBlank(this.accountName)) {

				logger.debug("Found Azure Account Name in properties file");

			} else {
				throw new Exception(
						"Azure Account Name not specified in properties");
			}
			// check if container name is specified
			if (StringUtils.isNotBlank(this.containerName)) {

				logger.debug("Found Azure Container Name in properties file");

			} else {
				throw new Exception(
						"Azure Container Name not specified in properties");
			}
			// Build Azure connection string
			// Connection method is always assumed to be HTTPS
			String storageConnectionString = "DefaultEndpointsProtocol=https;"
					+ "AccountName=" + this.accountName + ";AccountKey="
					+ this.accessKey;

			logger.debug("Azure Connection String: " + storageConnectionString);
			
			//Initialize Azure connection
			this.storageAccount = CloudStorageAccount.parse(storageConnectionString);
			this.blobClient = storageAccount.createCloudBlobClient();
			this.container = blobClient.getContainerReference(containerName);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}


	public void setAccountName(String accountName) {
		this.accountName = accountName;
	}

	public void setContainerName(String containerName) {
		this.containerName = containerName;
	}

	public void setRootDirectory(String rootDirectory) {

		String dir = rootDirectory;
		if (dir.startsWith("/")) {
			dir = dir.substring(1);
		}

		this.rootDirectory = dir;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	protected ContentWriter getWriterInternal(
			ContentReader existingContentReader, String newContentUrl) {

		String contentUrl = newContentUrl;

		if (StringUtils.isBlank(contentUrl)) {
			contentUrl = createNewUrl();
		}

		String key = makePathToObject(contentUrl);

		return new AzureContentWriter(key, contentUrl,
				existingContentReader, this.container);

	}

	public static String createNewUrl() {

		Calendar calendar = new GregorianCalendar();
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1; // 0-based
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		// create the URL
		StringBuilder sb = new StringBuilder(20);
		sb.append(FileContentStore.STORE_PROTOCOL)
				.append(ContentStore.PROTOCOL_DELIMITER).append(year)
				.append('/').append(month).append('/').append(day).append('/')
				.append(hour).append('/').append(minute).append('/')
				.append(GUID.generate()).append(".bin");
		String newContentUrl = sb.toString();
		// done
		return newContentUrl;

	}

	private String makePathToObject(String contentUrl) {
		// take just the part after the protocol
		Pair<String, String> urlParts = super.getContentUrlParts(contentUrl);
		String protocol = urlParts.getFirst();
		String relativePath = urlParts.getSecond();
		// Check the protocol
		if (!protocol.equals(FileContentStore.STORE_PROTOCOL)) {
			throw new UnsupportedContentUrlException(this, protocol
					+ PROTOCOL_DELIMITER + relativePath);
		}

		return rootDirectory + "/" + relativePath;

	}

	@Override
	public boolean delete(String contentUrl) {

		try {
			String pathToObject = makePathToObject(contentUrl);
			logger.debug("Deleting object from Azure with url: " + contentUrl
					+ ", pathToObject: " + pathToObject);
			
			CloudBlockBlob blob = container.getBlockBlobReference(pathToObject);
			blob.deleteIfExists();

			return true;
		} catch (Exception e) {
			logger.error("Error deleting Azure Object", e);
		}

		return false;

	}

	/**
	 * Publishes an event to the application context that will notify any
	 * interested parties of the existence of this content store.
	 *
	 * @param context
	 *            the application context
	 * @param extendedEventParams
	 */
	private void publishEvent(ApplicationContext context,
			Map<String, Serializable> extendedEventParams) {
		context.publishEvent(new ContentStoreCreatedEvent(this,
				extendedEventParams));
	}

	public void onApplicationEvent(ApplicationEvent event) {
		// Once the context has been refreshed, we tell other interested beans
		// about the existence of this content store
		// (e.g. for monitoring purposes)
		if (event instanceof ContextRefreshedEvent
				&& event.getSource() == this.applicationContext) {
			publishEvent(
					((ContextRefreshedEvent) event).getApplicationContext(),
					Collections.<String, Serializable> emptyMap());
		}
	}
}
