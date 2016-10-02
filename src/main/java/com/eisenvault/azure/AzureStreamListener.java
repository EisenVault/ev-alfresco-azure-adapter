package com.eisenvault.azure;

import java.io.File;
import java.io.FileInputStream;

import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentStreamListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.azure.storage.blob.CloudBlob;

public class AzureStreamListener implements ContentStreamListener {

    private static final Log logger = LogFactory.getLog(AzureStreamListener.class);

    private AzureContentWriter writer;

    public AzureStreamListener(AzureContentWriter writer) {

        this.writer = writer;

    }

    @Override
    public void contentStreamClosed() throws ContentIOException {

        File file = writer.getTempFile();
        long size = file.length();
        writer.setSize(size);

        try {

            logger.debug("Writing to Azure:" + writer.getContainerName() + "/" + writer.getPathToObject());
            CloudBlob transferManager = writer.getWritableObject();

            //transferManager.upload(new FileInputStream(writer.getPathToObject()),size);
            transferManager.upload(new FileInputStream(file.getAbsolutePath()),size);

        } catch (Exception e) {
            logger.error("AzureStreamListener Failed to Upload File", e);
        }

    }
}