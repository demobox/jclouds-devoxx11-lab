/*
 * Copyright 2011 Andrew Phillips, Andrew Kennedy.
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
import java.net.URI;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.collect.ImmutableSet;

/**
 * @author aphillips, grkvlt
 * @since 2 Nov 2011
 */
public class ImageUploaderC {
    public static final String CONTAINER_NAME = "test-container-1";
    public static final String BLOB_NAME = "uploadedImage";

    private final BlobStoreContext ctx;
    private final BlobStore store;
    
    public ImageUploaderC(String provider, String identity, String credential) {
        ctx = new BlobStoreContextFactory().createContext(provider, identity, credential, ImmutableSet.of(new Log4JLoggingModule()));
        System.out.printf("Provider '%s' uses %s consistency%n", provider, ctx.getConsistencyModel());
        store = ctx.getBlobStore();
    }
    
    public void uploadImage(File image) throws IOException {
        createContainer(CONTAINER_NAME);
        String blobName = storeBlob(CONTAINER_NAME, image);
        System.out.printf("Now please open '%s' in a browser%nPress any key, then <enter>: ", getImageUri(blobName).toASCIIString());
        System.in.read(); // pause
        tryDeleteContainer(CONTAINER_NAME);
    }

    public void createContainer(String containerName) {
        System.out.printf("Creating public container '%s'%n", containerName);
        store.createContainerInLocation(null, containerName, CreateContainerOptions.Builder.publicRead());
    }

    public String storeBlob(String containerName, File image) {
        String blobName = image.getName();
        System.out.printf("Storing file '%s' as '%s'%n", image.getPath(), blobName);
        store.putBlob(containerName, store.blobBuilder(blobName).payload(image).contentType("image/jpeg").build());
        return blobName;
    }

    public URI getImageUri(String blobName) {
        return store.blobMetadata(CONTAINER_NAME, blobName).getPublicUri();
    }
    
    public void tryDeleteContainer(String containerName) {
        System.out.printf("Deleting public container '%s'%n", containerName);
        try {
            store.deleteContainer(containerName);
        } catch (Exception exception) {
            System.err.printf("Unable to delete container due to: %s%n", exception.getMessage());
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
        ImageUploaderC uploader = new ImageUploaderC(args[0], args[1], args[2]);
        try {
            uploader.uploadImage(new File("src/main/resources/cloud.jpg"));
        } finally {
            uploader.cleanup();
        }
    }
}
