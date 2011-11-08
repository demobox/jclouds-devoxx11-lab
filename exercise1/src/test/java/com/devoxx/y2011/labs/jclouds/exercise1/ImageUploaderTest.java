/*
 * Copyright 2011 Andrew Kennedy
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

import static org.testng.Assert.*;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

/**
 * Tests for each {@code ImageUploader} implementation.
 *
 * Run using Maven as follows (using Ninefold provider):
 * <pre>
 * $ export JCLOUDS_IDENTITY_NINEFOLD_STORAGE=xxx
 * $ export JCLOUDS_CREDENTIAL_NINEFOLD_STORAGE=yyy
 * $ mvn test -Djclouds.provider=ninefold-storage
 * </pre>
 * or
 * <pre>
 * $ mvn test -Djclouds.provider=ninefold-storage -Djclouds.identity=xxx -Djclouds.credential=yyy
 * </pre>
 *
 * @author grkvlt
 */
public class ImageUploaderTest {
    private String provider;
    private String identity;
    private String credential;

    @BeforeMethod
    public void setupCredentials() {
        provider = System.getProperty("jclouds.provider");
        identity = System.getProperty("jclouds.identity");
        credential = System.getProperty("jclouds.credential");
        if (identity == null || credential == null) {
	        identity = System.getenv("JCLOUDS_IDENTITY_" + CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_UNDERSCORE, provider));
	        credential = System.getenv("JCLOUDS_CREDENTIAL_" + CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_UNDERSCORE, provider));
        }
        System.setProperty("jclouds.identity", Strings.nullToEmpty(identity));
        System.setProperty("jclouds.credential", Strings.nullToEmpty(credential));
    }

    @Test
    public void testUploader() throws Exception {
        ImageUploader uploader = new ImageUploader(provider, identity, credential);
        String imageUrl = "";
        try {
            File image = new File("src/main/resources/cloud.jpg");
            uploader.createContainer(ImageUploader.CONTAINER_NAME);
            uploader.storeBlob(ImageUploader.CONTAINER_NAME, ImageUploader.BLOB_NAME, image);
            URI uri = uploader.getImageUri();
            imageUrl = uri.toASCIIString();
            System.out.printf("URL is '%s'%n", imageUrl);

            // Check URL contents
            assertTrue(imageUrl.contains(ImageUploader.CONTAINER_NAME));
            assertTrue(imageUrl.contains(ImageUploader.BLOB_NAME));

            // Check image was stored OK
	        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
	        assertEquals(200, connection.getResponseCode());

            // Check blob contents are correct
            byte[] file = Files.toByteArray(image);
            byte[] blob = ByteStreams.toByteArray(uri.toURL().openStream());
            assertEquals(file, blob);
        } finally {
            uploader.tryDeleteContainer(ImageUploader.CONTAINER_NAME);
            uploader.cleanup();
        }

        // Check container has been deleted
        HttpURLConnection connection = (HttpURLConnection) new URL(imageUrl).openConnection();
        assertEquals(404, connection.getResponseCode());
    }

    @Test
    public void testUploaderB() throws Exception {
        ImageUploaderB uploader = new ImageUploaderB(provider, identity, credential);
        String imageUrl = "";
        try {
            File image = new File("src/main/resources/cloud.jpg");
            uploader.createContainer(ImageUploaderB.CONTAINER_NAME);
            uploader.storeBlob(ImageUploaderB.CONTAINER_NAME, ImageUploaderB.BLOB_NAME, image);
            URI uri = uploader.getImageUri();
            imageUrl = uri.toASCIIString();
            System.out.printf("URL is '%s'%n", imageUrl);

            // Check URL contents
            assertTrue(imageUrl.contains(ImageUploaderB.CONTAINER_NAME));
            assertTrue(imageUrl.contains(ImageUploaderB.BLOB_NAME));

            // Check image was stored OK
	        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
	        assertEquals(200, connection.getResponseCode());
            assertEquals(uri.toURL().openConnection().getContentType(), "image/jpeg");

            // Check blob contents are correct
            byte[] file = Files.toByteArray(image);
            byte[] blob = ByteStreams.toByteArray(uri.toURL().openStream());
            assertEquals(file, blob);
        } finally {
            uploader.tryDeleteContainer(ImageUploaderB.CONTAINER_NAME);
            uploader.cleanup();
        }

        // Check container has been deleted
        HttpURLConnection connection = (HttpURLConnection) new URL(imageUrl).openConnection();
        assertEquals(404, connection.getResponseCode());
    }

    @Test
    public void testUploaderC() throws Exception {
        ImageUploaderC uploader = new ImageUploaderC(provider, identity, credential);
        String imageUrl = "";
        try {
            File image = new File("src/main/resources/cloud.jpg");
            uploader.createContainer(ImageUploaderC.CONTAINER_NAME);
            String blobName = uploader.storeBlob(ImageUploaderC.CONTAINER_NAME, image);
            URI uri = uploader.getImageUri(blobName);
            imageUrl = uri.toASCIIString();
            System.out.printf("URL is '%s'%n", imageUrl);

            // Check URL contents
            assertTrue(imageUrl.contains(ImageUploaderC.CONTAINER_NAME));
            assertTrue(imageUrl.contains(image.getName()));

            // Check image was stored OK
	        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
	        assertEquals(200, connection.getResponseCode());
            assertEquals(uri.toURL().openConnection().getContentType(), "image/jpeg");

            // Check blob contents are correct
            byte[] file = Files.toByteArray(image);
            byte[] blob = ByteStreams.toByteArray(uri.toURL().openStream());
            assertEquals(file, blob);
        } finally {
            uploader.tryDeleteContainer(ImageUploaderC.CONTAINER_NAME);
            uploader.cleanup();
        }

        // Check container has been deleted
        HttpURLConnection connection = (HttpURLConnection) new URL(imageUrl).openConnection();
        assertEquals(404, connection.getResponseCode());
    }
}
