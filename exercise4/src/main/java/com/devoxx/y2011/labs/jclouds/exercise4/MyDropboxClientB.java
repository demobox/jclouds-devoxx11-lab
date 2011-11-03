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
package com.devoxx.y2011.labs.jclouds.exercise4;

import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static org.jclouds.blobstore.options.ListContainerOptions.Builder.inDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.jclouds.blobstore.BlobMap;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * @author aphillips
 * @since 2 Nov 2011
 *
 */
public class MyDropboxClientB {
    private static final String RESOURCE_DIR = "src/main/resources";
    private static final String CONTAINER_NAME = "test-container-4";
    
    private final BlobStoreContext ctx;
    private final BlobStore store;
    
    public MyDropboxClientB(String provider, String identity, String credential) {
        ctx = new BlobStoreContextFactory().createContext(provider, identity, credential, 
                ImmutableSet.of(new Log4JLoggingModule()));
        store = ctx.getBlobStore();
        createContainerIfNeeded(store);
    }
    
    private static void createContainerIfNeeded(BlobStore store) {
        if (!store.containerExists(CONTAINER_NAME)) {
            System.out.format("Creating non-existent container '%s'%n", CONTAINER_NAME);
            store.createContainerInLocation(null, CONTAINER_NAME);
        }
    }

    public void uploadFiles(String directory, List<File> files) throws IOException {
        BlobMap map = ctx.createBlobMap(CONTAINER_NAME, inDirectory(directory));
        final BlobBuilder builder = map.blobBuilder();
        List<Blob> blobs = transform(files, new Function<File, Blob>() {
                @Override
                public Blob apply(File input) {
                    return builder.name(input.getName()).payload(input).build();
                }
            });
        System.out.format("Uploading %s to directory '%s'%n", files, directory);
        map.putAll(uniqueIndex(blobs, new Function<Blob, String>() {
            @Override
            public String apply(Blob input) {
                return input.getMetadata().getName();
            }
        }));
    }

    public List<String> list(String directory) {
        System.out.format("Getting directory listing for '%s'%n", directory);
        PageSet<? extends StorageMetadata> partial = 
            store.list(CONTAINER_NAME, inDirectory(directory));
        if (partial.getNextMarker() != null) {
            System.err.println("Unable to retrieve entire listing");
        }
        return ImmutableList.copyOf(Iterables.transform(partial,
                new Function<StorageMetadata, String>() {
                    @Override
                    public String apply(StorageMetadata input) {
                        return input.getName();
                    }
                }));
    }
    
    
    public void cleanup() {
        tryDeleteContainer(store);
        ctx.close();
    }
    
    private static void tryDeleteContainer(BlobStore store) {
        try {
            store.deleteContainer(CONTAINER_NAME);
        } catch (Exception exception) {
            System.err.format("Unable to delete container due to: %s%n", exception.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.format("%nUsage: %s <provider> <identity> <credential>%n", MyDropboxClientB.class.getSimpleName());
            System.exit(1);
        }
        MyDropboxClientB dropbox = new MyDropboxClientB(args[0], args[1], args[2]);
        try {
            dropbox.uploadFiles("docs", toFilesInResources("s3-gsg.pdf", "s3-qrc.pdf"));
            dropbox.uploadFiles("misc", toFilesInResources("cloud.jpg", "programmer-jokes.txt"));
            System.out.format("In folder 'docs': %s%n", dropbox.list("docs"));
            System.out.format("In folder 'misc': %s%n", dropbox.list("misc"));
        } finally {
            dropbox.cleanup();
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
