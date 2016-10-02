package com.eisenvault.azure;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import org.alfresco.repo.content.AbstractContentWriter;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.util.GUID;
import org.alfresco.util.TempFileProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class AzureContentWriter extends AbstractContentWriter {

	private static final Log logger = LogFactory
			.getLog(AzureContentWriter.class);

	// private TransferManager transferManager;
	private CloudBlobContainer container;
	private CloudBlob blob;
	private String pathToObject;

	private File tempFile;
	private long size;

	public AzureContentWriter(String pathToObject, String contentUrl,
			ContentReader existingContentReader, CloudBlobContainer container) {
		super(contentUrl, existingContentReader);
		try {
			this.pathToObject = pathToObject;
			this.container = container;
			// this.transferManager = transferManager;
			this.blob = container.getBlockBlobReference(this.pathToObject);
			addListener(new AzureStreamListener(this));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected ContentReader createReader() throws ContentIOException {
		return new AzureContentReader(pathToObject, getContentUrl(), container);
	}

	@Override
	protected WritableByteChannel getDirectWritableChannel()
			throws ContentIOException {

		try {

			String uuid = GUID.generate();
			logger.debug("AzureContentWriter Creating Temp File: uuid=" + uuid);
			tempFile = TempFileProvider.createTempFile(uuid, ".bin");
			OutputStream os = new FileOutputStream(tempFile);
			logger.debug("AzureContentWriter Returning Channel to Temp File: uuid="
					+ uuid);
			return Channels.newChannel(os);
		} catch (Throwable e) {
			throw new ContentIOException(
					"AzureContentWriter.getDirectWritableChannel(): Failed to open channel. "
							+ this, e);
		}

	}

	@Override
	public long getSize() {
		return this.size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public CloudBlob getWritableObject() {
		return blob;
	}

	public String getContainerName() {
		return this.container.getName();
	}

	public String getPathToObject() {
		return this.pathToObject;
	}

	public File getTempFile() {
		return tempFile;
	}
}
