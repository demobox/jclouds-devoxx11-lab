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
package com.devoxx.y2011.labs.jclouds.exercise2;

import java.io.File;
import java.io.IOException;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.collect.ImmutableSet;

/**
 * @author aphillips
 * @since 2 Nov 2011
 *
 */
public class ImageUploader {
    private final BlobStoreContext ctx;
    
    public ImageUploader(String provider, String identity, String credential) {
        ctx = new BlobStoreContextFactory().createContext(provider, identity, credential, 
                ImmutableSet.of(new Log4JLoggingModule()));
    }
    
    public void uploadImage(File image) throws IOException {
        BlobStore store = ctx.getBlobStore();
        final String containerName = "test-container-2";
        final String blobName = "uploadedImage";
        System.out.format("Creating public container '%s'%n", containerName);
        store.createContainerInLocation(null, containerName, CreateContainerOptions.Builder.publicRead());
        store.putBlob(containerName, store.blobBuilder(blobName).payload(image).build());
        System.out.format("Now please open '%s' in a browser%nPress any key, then <enter>: ", 
                store.blobMetadata(containerName, blobName).getPublicUri());
        // pause
        System.in.read();
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
            System.out.format("%nUsage: %s <provider> <identity> <credential>%n", ImageUploader.class.getSimpleName());
            System.exit(1);
        }
        ImageUploader uploader = new ImageUploader(args[0], args[1], args[2]);
        try {
            uploader.uploadImage(new File("src/main/resources/cloud.jpg"));
        } finally {
            uploader.cleanup();
        }
    }
}
