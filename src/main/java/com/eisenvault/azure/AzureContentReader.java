package com.eisenvault.azure;

import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.alfresco.repo.content.AbstractContentReader;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class AzureContentReader extends AbstractContentReader {

	private static final Log logger = LogFactory
			.getLog(AzureContentReader.class);

	private String pathToObject;
	private CloudBlobContainer container;
	private CloudBlob fileObject;
	private BlobProperties fileObjectMetadata;

	/**
	 * @param pathToObject -- string with relative path to object in the Azure container
	 * @param contentUrl
	 *            the content URL - this should be relative to the root of the
	 *            store
	 * @param container - the Azure Container's reference of type CloudBlobContainer
	 */
	protected AzureContentReader(String pathToObject, String contentUrl,
			CloudBlobContainer container) {
		super(contentUrl);
		this.pathToObject = pathToObject;
		this.container = container;
		this.fileObject = getObject();
		this.fileObjectMetadata = getObjectMetadata(this.fileObject);
	}

	@Override
	protected ContentReader createReader() throws ContentIOException {

		logger.debug("Called createReader for contentUrl -> " + getContentUrl()
				+ ", pathToObject: " + pathToObject);
		return new AzureContentReader(pathToObject, getContentUrl(), container);
	}

	@Override
	protected ReadableByteChannel getDirectReadableChannel()
			throws ContentIOException {

		if (!exists()) {
			throw new ContentIOException(
					"Content object does not exist on Azure");
		}

		try {
			return Channels.newChannel(fileObject.openInputStream());
		} catch (Exception e) {
			throw new ContentIOException(
					"Unable to retrieve content object from Azure", e);
		}

	}

	@Override
	public boolean exists() {
		return fileObjectMetadata != null;
	}

	@Override
	public long getLastModified() {

		if (!exists()) {
			return 0L;
		}

		return fileObjectMetadata.getLastModified().getTime();

	}

	@Override
	public long getSize() {

		if (!exists()) {
			return 0L;
		}

		return fileObjectMetadata.getLength();
	}

	private CloudBlob getObject() {

		CloudBlob object = null;

		try {
			logger.debug("GETTING OBJECT - Path: " + pathToObject);
			object = container.getBlobReferenceFromServer(pathToObject);
		} catch (Exception e) {
			logger.error("Unable to fetch Azure Blob Object", e);
		}

		return object;
	}

	private BlobProperties getObjectMetadata(CloudBlob object) {

		BlobProperties metadata = null;

		if (object != null) {
			metadata = object.getProperties();
		}

		return metadata;

	}
}
