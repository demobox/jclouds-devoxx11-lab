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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * @author aphillips
 * @since 2 Nov 2011
 *
 */
public class MultiFileUploader {
    private static final String RESOURCE_DIR = "src/main/resources";
    
    private final BlobStoreContext ctx;
    
    public MultiFileUploader(String provider, String identity, String credential) {
        ctx = new BlobStoreContextFactory().createContext(provider, identity, credential, 
                ImmutableSet.of(new Log4JLoggingModule()));
    }
    
    public void uploadFiles(List<File> files) throws IOException {
        BlobStore store = ctx.getBlobStore();
        final String containerName = "test-container-3";
        long startTimeMillis = System.currentTimeMillis();
        System.out.format("Starting upload of %d files%n", files.size());
        for (File file : files) {
            String filename = file.getName();
            System.out.format("Uploading '%s'...%n", filename);
            store.putBlob(containerName, 
                    store.blobBuilder(filename).payload(file).build());
        }
        System.out.format("Uploaded %d files in %dms", files.size(), 
                System.currentTimeMillis() - startTimeMillis);
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
            System.out.format("%nUsage: %s <provider> <identity> <credential>%n", MultiFileUploader.class.getSimpleName());
            System.exit(1);
        }
        MultiFileUploader uploader = new MultiFileUploader(args[0], args[1], args[2]);
        try {
            uploader.uploadFiles(toFilesInResources("s3-api.pdf", "s3-dg.pdf", "s3-gsg.pdf",
                    "s3-qrc.pdf", "s3-ug.pdf"));
        } finally {
            uploader.cleanup();
        }
    }
    
    private static List<File> toFilesInResources(String... filenames) {
        return Lists.transform(Arrays.asList(filenames), new Function<String, File>() {
                @Override
                public File apply(String input) {
                    return new File(RESOURCE_DIR + File.separator + input);
                }
            });
    }
}
