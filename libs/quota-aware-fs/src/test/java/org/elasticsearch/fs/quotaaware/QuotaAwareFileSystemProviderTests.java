/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.fs.quotaaware;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts.Limit;
import org.elasticsearch.common.SuppressForbidden;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.security.PrivilegedActionException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

@Limit(bytes = 10000)
@SuppressForbidden(reason = "accesses the default filesystem by design")
public class QuotaAwareFileSystemProviderTests extends LuceneTestCase {

    public void testInitiallyNoQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = FileSystems.getDefault().provider();
        Throwable cause = null;

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            fail(); //
        } catch (PrivilegedActionException e) {
            cause = e.getCause();
        }
        assertTrue("Should be FileNotFoundException", cause instanceof NoSuchFileException);
    }

    public void testBasicQuotaFile() throws Exception {
        doValidFileTest(500, 200);
    }

    public void testUpdateQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(500, provider.getTotal());
            assertEquals(200, provider.getRemaining());
            writeQuota(450, 150, systemProvider, quotaFile);
            withRetry(2000, 500, () -> {
                assertEquals(450, provider.getTotal());
                assertEquals(150, provider.getRemaining());
            });
        }
    }

    public void testRepeatedUpdate() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);
        Random random = new Random();
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            for (int i = 0; i < 10; i++) {
                long expectedTotal = Math.abs(random.nextLong());
                long expectedRemaining = Math.abs(random.nextLong());
                writeQuota(expectedTotal, expectedRemaining, systemProvider, quotaFile);
                withRetry(2000, 100, () -> {
                    assertEquals(expectedTotal, provider.getTotal());
                    assertEquals(expectedRemaining, provider.getRemaining());
                });
            }
        }
    }

    public void testEventuallyMissingQuotaFile() throws Exception {

        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(500, provider.getTotal());
            assertEquals(200, provider.getRemaining());

            systemProvider.delete(quotaFile);

            withRetry(2000, 500, () -> {
                boolean gotError = false;
                try {
                    provider.getTotal();
                } catch (AssertionError e) {
                    gotError = true;
                }
                assertTrue(gotError);
            });
        }
    }

    public void testEventuallyMalformedQuotaFile() throws Exception {

        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(500L, 200L, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(500, provider.getTotal());
            assertEquals(200, provider.getRemaining());

            try (
                OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, TRUNCATE_EXISTING);
                OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
                PrintWriter printWriter = new PrintWriter(streamWriter)
            ) {
                printWriter.write("This is not valid properties file syntax");
            }

            withRetry(2000, 500, () -> {
                boolean gotError = false;
                try {
                    provider.getTotal();
                } catch (AssertionError e) {
                    gotError = true;
                }
                assertTrue(gotError);
            });
        }
    }

    public void testHighQuotaFile() throws Exception {
        doValidFileTest(Long.MAX_VALUE - 1L, Long.MAX_VALUE - 2L);
    }

    public void testMalformedNumberInQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        quota.setProperty("total", "ThisNotANumber");
        quota.setProperty("remaining", "1");
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#malformedNumberInQuotaFile");
        }

        expectThrows(NumberFormatException.class, () -> new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri()));
    }

    public void testMalformedQuotaFile() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();

        try (
            OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW);
            OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
            PrintWriter printWriter = new PrintWriter(streamWriter)
        ) {
            printWriter.write("This is not valid properties file syntax");
        }

        expectThrows(Exception.class, () -> new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri()));
    }

    public void testFileStoreLimited() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        long expectedTotal = 500;
        long expectedRemaining = 200;
        quota.setProperty("total", Long.toString(expectedTotal));
        quota.setProperty("remaining", Long.toString(expectedRemaining));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#fileStoreLimited");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            Path path = createTempFile();
            FileStore fileStore = provider.getFileStore(path);
            assertEquals(expectedTotal, fileStore.getTotalSpace());
            assertEquals(expectedRemaining, fileStore.getUsableSpace());
            assertEquals(expectedRemaining, fileStore.getUnallocatedSpace());
        }
    }

    public void testFileStoreNotLimited() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(Long.MAX_VALUE));
        quota.setProperty("remaining", Long.toString(Long.MAX_VALUE));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#fileStoreNotLimited");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            Path path = createTempFile();
            FileStore fileStore = provider.getFileStore(path);
            FileStore unLimitedStore = systemProvider.getFileStore(path);
            assertEquals(unLimitedStore.getTotalSpace(), fileStore.getTotalSpace());
            assertEquals(unLimitedStore.getUsableSpace(), fileStore.getUsableSpace());
            assertEquals(unLimitedStore.getUnallocatedSpace(), fileStore.getUnallocatedSpace());
        }
    }

    public void testDefaultFilesystemIsPreinitialized() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(Long.MAX_VALUE));
        quota.setProperty("remaining", Long.toString(Long.MAX_VALUE));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#defaultFilesystemIsPreinitialized");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertNotNull(provider.getFileSystem(new URI("file:///")));
        }
    }

    /**
     * Mimics a cyclic reference that may happen when
     * {@link QuotaAwareFileSystemProvider} is installed as the default provider
     * in the JVM and the delegate provider references
     * {@link FileSystems#getDefault()} by for instance relying on
     * {@link File#toPath()}
     */
    public void testHandleReflexiveDelegate() throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        DelegatingProvider cyclicProvider = new DelegatingProvider(systemProvider) {
            @Override
            public Path getPath(URI uri) {
                try {
                    return cyclicReference.getFileSystem(new URI("file:///")).getPath(uri.getPath());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(Long.MAX_VALUE));
        quota.setProperty("remaining", Long.toString(Long.MAX_VALUE));
        try (OutputStream stream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE_NEW)) {
            quota.store(stream, "QuotaFile for: QuotaAwareFileSystemProviderTest#testHandleReflexiveDelegate");
        }
        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(cyclicProvider, quotaFile.toUri())) {
            cyclicProvider.cyclicReference = provider;
            assertNotNull(provider.getPath(new URI("file:///")));
        }
    }

    /**
     * A simple purely delegating provider, allows tests to only override the
     * methods they need custom behaviour on.
     *
     */
    private static class DelegatingProvider extends FileSystemProvider {
        private FileSystemProvider provider;
        /**
         * An optional field for subclasses that need to test cyclic references
         */
        protected FileSystemProvider cyclicReference;

        DelegatingProvider(FileSystemProvider provider) {
            this.provider = provider;
            this.cyclicReference = provider;
        }

        @Override
        public String getScheme() {
            return provider.getScheme();
        }

        @Override
        public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
            return provider.newFileSystem(uri, env);
        }

        @Override
        public FileSystem getFileSystem(URI uri) {
            return provider.getFileSystem(uri);
        }

        @Override
        public Path getPath(URI uri) {
            return provider.getPath(uri);
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
            return provider.newByteChannel(path, options, attrs);
        }

        @Override
        public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
            return provider.newDirectoryStream(dir, filter);
        }

        @Override
        public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
            provider.createDirectory(dir, attrs);
        }

        @Override
        public void delete(Path path) throws IOException {
            provider.delete(path);
        }

        @Override
        public void copy(Path source, Path target, CopyOption... options) throws IOException {
            provider.copy(source, target, options);
        }

        @Override
        public void move(Path source, Path target, CopyOption... options) throws IOException {
            provider.move(source, target, options);
        }

        @Override
        public boolean isSameFile(Path path, Path path2) throws IOException {
            return provider.isSameFile(path, path2);
        }

        @Override
        public boolean isHidden(Path path) throws IOException {
            return provider.isHidden(path);
        }

        @Override
        public FileStore getFileStore(Path path) throws IOException {
            return provider.getFileStore(path);
        }

        @Override
        public void checkAccess(Path path, AccessMode... modes) throws IOException {
            provider.checkAccess(path, modes);
        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
            return provider.getFileAttributeView(path, type, options);
        }

        @Override
        public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
            return provider.readAttributes(path, type, options);
        }

        @Override
        public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
            return provider.readAttributes(path, attributes, options);
        }

        @Override
        public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
            provider.setAttribute(path, attribute, value, options);
        }

    }

    private void doValidFileTest(long expectedTotal, long expectedRemaining) throws Exception {
        Path quotaFile = createTempDir().resolve("quota.properties");
        FileSystemProvider systemProvider = quotaFile.getFileSystem().provider();
        writeQuota(expectedTotal, expectedRemaining, systemProvider, quotaFile);

        try (QuotaAwareFileSystemProvider provider = new QuotaAwareFileSystemProvider(systemProvider, quotaFile.toUri())) {
            assertEquals(expectedTotal, provider.getTotal());
            assertEquals(expectedRemaining, provider.getRemaining());
        }
    }

    private void writeQuota(long expectedTotal, long expectedRemaining, FileSystemProvider systemProvider, Path quotaFile)
        throws IOException {
        Properties quota = new Properties();
        quota.setProperty("total", Long.toString(expectedTotal));
        quota.setProperty("remaining", Long.toString(expectedRemaining));
        try (OutputStream outputStream = systemProvider.newOutputStream(quotaFile, WRITE, CREATE, TRUNCATE_EXISTING)) {
            // Ideally this would use atomic write and the scala allocator does,
            // but the parsing logic should be able to deal with it in either
            // case.
            quota.store(outputStream, "QuotaFile for: QuotaAwareFileSystemProviderTest#doValidFileTest");
        }
    }

    public static void withRetry(int maxMillis, int interval, Runnable func) throws Exception {
        long endBy = System.currentTimeMillis() + maxMillis;

        while (true) {
            try {
                func.run();
                break;
            } catch (AssertionError | Exception e) {
                if (System.currentTimeMillis() + interval < endBy) {
                    Thread.sleep(interval);
                    continue;
                }
                throw new IllegalStateException("Retry timed out after [" + maxMillis + "]ms", e);
            }
        }

    }

}
