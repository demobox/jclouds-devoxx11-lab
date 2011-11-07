/*
 * @(#)BlobWriterReader.java     2 Nov 2011
 *
 * Copyright © 2010 Andrew Phillips.
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */
package com.devoxx.y2011.labs.jclouds.exercise3;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author aphillips
 * @since 2 Nov 2011
 *
 */
public class FileUploader {
    private static final int QUERY_RETRY_INTERVAL_MILLIS = 100;
    
    private final BlobStoreContext ctx;
    
    public FileUploader(String provider, String identity, String credential) {
        // create context for filesystem container
        Properties config = new Properties();
        config.put(FilesystemConstants.PROPERTY_BASEDIR, 
                checkNotNull(System.getProperty("java.io.tmpdir"), "java.io.tmpdir"));
        ctx = new BlobStoreContextFactory().createContext(provider, identity, credential, 
                ImmutableSet.of(new Log4JLoggingModule()), config);
    }
    
    public void uploadFile(File file) throws IOException, InterruptedException, ExecutionException {
        AsyncBlobStore store = ctx.getAsyncBlobStore();
        final String containerName = "test-container-3";
        long fileSize = file.length();
        System.out.format("Starting upload of %d bytes%n", fileSize);
        String filename = file.getName();
        ListenableFuture<String> putBlobOperation = store.putBlob(containerName, store.blobBuilder(filename).payload(file).build());
        waitUntilExists(store, containerName, filename);
        byte[] payloadRead = ByteStreams.toByteArray(
                store.getBlob(containerName, filename).get().getPayload().getInput());
        System.out.format("Retrieved blob size: %d bytes%n", payloadRead.length);
        waitForUpload(putBlobOperation);
        payloadRead = ByteStreams.toByteArray(
                store.getBlob(containerName, filename).get().getPayload().getInput());
        System.out.format("Retrieved blob size now: %d bytes%n", payloadRead.length);
        tryDeleteContainer(store, containerName);
    }
    
    private static void waitUntilExists(AsyncBlobStore store, String containerName, String blobName) throws InterruptedException, ExecutionException {
        while (!store.blobExists(containerName, blobName).get()) {
            TimeUnit.MILLISECONDS.sleep(QUERY_RETRY_INTERVAL_MILLIS);
            System.out.println("Waiting for blob to 'exist'");
        }
        System.out.println("Blob exists");
    }
    
    private static void waitForUpload(ListenableFuture<String> uploadOperation) throws InterruptedException, ExecutionException {
        System.out.println("Waiting for upload to complete");
        uploadOperation.get();
        System.out.println("Upload completed");        
    }
    
    private static void tryDeleteContainer(AsyncBlobStore store, String containerName) {
        try {
            // throws IOException without the "clearContainer" call
            store.clearContainer(containerName).get();
            store.deleteContainer(containerName).get();
        } catch (Exception exception) {
            System.err.format("Unable to delete container due to: %s%n", exception.getMessage());
        }
    }
    
    public void cleanup() {
        ctx.close();
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.format("%nUsage: %s <provider> <identity> <credential>%n", FileUploader.class.getSimpleName());
            System.exit(1);
        }
        FileUploader uploader = new FileUploader(args[0], args[1], args[2]);
        try {
            uploader.uploadFile(new File("src/main/resources/s3-qrc.pdf"));
        } finally {
            uploader.cleanup();
        }
    }
}
