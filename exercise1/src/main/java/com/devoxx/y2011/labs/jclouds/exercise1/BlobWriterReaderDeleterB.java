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
package com.devoxx.y2011.labs.jclouds.exercise1;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

/**
 * @author aphillips
 * @since 2 Nov 2011
 *
 */
public class BlobWriterReaderDeleterB {
    private final BlobStoreContext ctx;
    
    public BlobWriterReaderDeleterB(String provider, String identity, String credential) {
        ctx = new BlobStoreContextFactory().createContext(provider, identity, credential, 
                ImmutableSet.of(new Log4JLoggingModule()));
    }
    
    public void writeReadAndDelete(byte[] payload, int numIterations) throws IOException {
        BlobStore store = ctx.getBlobStore();
        final String containerName = "test-container";
        final String blobNamePrefix = "test-blob";
        String blobName; 
        for (int i = 0; i < numIterations; i++) {
            System.out.format("---%nWrite/read/delete cycle #%d%n", i);
            blobName = blobNamePrefix + i;
            System.out.format("Writing blob '%s'...%n", blobName);
            store.putBlob(containerName, store.blobBuilder(blobName).payload(payload).build());
            System.out.println("Reading blob back...");
            byte[] payloadRead = ByteStreams.toByteArray(
                    store.getBlob(containerName, blobName).getPayload().getInput());
            if (!Arrays.equals(payload, payloadRead)) {
                System.err.format("Contents of blob read didn't match '%s' but were '%s'%n",
                        Arrays.toString(payload), Arrays.toString(payloadRead));
            } else {
                System.out.println("Blob read matches input");
            }
            System.out.println("Deleting blob...");
            store.removeBlob(containerName, blobName);
            if (store.blobExists(containerName, blobName)) {
                System.err.println("Blob still present even after 'removeBlob' call");
            } else {
                System.out.println("Blob no longer exists");
            }
        }
        tryDeleteContainer(store, containerName);
    }
    
    private static void tryDeleteContainer(BlobStore store, String containerName) {
        try {
            store.deleteContainer(containerName);
        } catch (Exception exception) {
            System.err.format("Unable to delete container due to: %s%n", exception.getMessage());
        }
    }
    
    public void cleanup() {
        ctx.close();
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.format("%nUsage: %s <provider> <identity> <credential>%n", BlobWriterReaderDeleterB.class.getSimpleName());
            System.exit(1);
        }
        BlobWriterReaderDeleterB readerWriter = new BlobWriterReaderDeleterB(args[0], args[1], args[2]);
        try {
            byte[] blob = Files.toByteArray(new File("src/main/resources/programmer-jokes.txt"));
            readerWriter.writeReadAndDelete(blob, 5);
        } finally {
            readerWriter.cleanup();
        }
    }
}
