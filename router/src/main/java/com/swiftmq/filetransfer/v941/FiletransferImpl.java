/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.filetransfer.v941;

import com.swiftmq.filetransfer.*;
import com.swiftmq.filetransfer.protocol.MessageBasedFactory;
import com.swiftmq.filetransfer.protocol.v941.*;
import com.swiftmq.filetransfer.v940.LinkBuilder;
import com.swiftmq.filetransfer.v940.LinkParser;
import com.swiftmq.tools.requestreply.RequestRegistry;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.*;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FiletransferImpl extends Filetransfer {
    private static final String DEFAULT_DIGEST = "MD5";

    JMSAccessorHolder accessorHolder = null;
    File file = null;
    String filename = null;
    String link = null;
    long expiration = 0;
    int deleteAfterNumberDownloads = 0;
    String password = null;
    String passwordHexDigest = null;
    boolean fileIsPrivate = false;
    boolean useOriginalFilename = true;
    File outputDir = null;
    int chunkLength = 0;
    String digestType = DEFAULT_DIGEST;
    int replyInterval = 10;
    Map<String, Object> properties = null;
    String selector = null;
    MessageBasedFactory messageBasedFactory = new ProtocolFactory();
    boolean closed = false;

    public FiletransferImpl(JMSAccessorHolder accessorHolder) {
        this.accessorHolder = accessorHolder;
    }

    private void checkState() {
        if (closed)
            throw new IllegalStateException("This object has already been closed.");
    }

    public Filetransfer withFile(File file) {
        checkState();
        this.file = file;
        return this;
    }

    public Filetransfer withFilename(String filename) {
        checkState();
        this.filename = filename;
        return this;
    }

    public Filetransfer withLink(String link) {
        checkState();
        this.link = link;
        return this;
    }

    public Filetransfer withDigestType(String digestType) {
        checkState();
        this.digestType = digestType;
        return this;
    }

    public Filetransfer withDeleteAfterNumberDownloads(int deleteAfterNumberDownloads) {
        checkState();
        this.deleteAfterNumberDownloads = deleteAfterNumberDownloads;
        return this;
    }

    public Filetransfer withExpiration(long expiration) {
        checkState();
        this.expiration = expiration;
        return this;
    }

    public Filetransfer withPassword(String password) {
        checkState();
        this.password = password;
        return this;
    }

    public Filetransfer withPasswordHexDigest(String passwordHexDigest) {
        checkState();
        this.passwordHexDigest = passwordHexDigest;
        return this;
    }

    public Filetransfer withFileIsPrivate(boolean fileIsPrivate) {
        checkState();
        this.fileIsPrivate = fileIsPrivate;
        return this;
    }

    public Filetransfer withOutputDirectory(File outputDir) {
        checkState();
        this.outputDir = outputDir;
        return this;
    }

    public Filetransfer withOriginalFilename(boolean withOriginalFilename) {
        checkState();
        this.useOriginalFilename = withOriginalFilename;
        return this;
    }

    public Filetransfer withProperties(Map<String, Object> properties) {
        checkState();
        this.properties = properties;
        return this;
    }

    public Filetransfer withSelector(String selector) {
        checkState();
        this.selector = selector;
        return this;
    }

    public Filetransfer withReplyInterval(int replyInterval) {
        checkState();
        this.replyInterval = replyInterval;
        return this;
    }

    public String send() throws Exception {
        return send(null);
    }

    public String send(ProgressListener progressListener) throws FiletransferException, JMSException, IOException, NoSuchAlgorithmException {
        checkState();
        if (file == null)
            throw new NullPointerException("File is not specified");
        if (!file.exists())
            throw new FileNotFoundException("File not found");
        if (file.isDirectory())
            throw new FiletransferException("File to transfer is a directory");
        long fileLength = file.length();
        if (fileLength == 0)
            throw new FiletransferException("File to transfer is empty");
        if (passwordHexDigest == null && password != null) {
            byte[] pwdigest = MessageDigest.getInstance(digestType).digest(password.getBytes());
            passwordHexDigest = Util.byteArrayToHexString(pwdigest);
        }
        FilePublishReply reply = (FilePublishReply) request(new FilePublishRequest(accessorHolder.replyQueue.getQueueName(), filename == null ? file.getName() : filename, accessorHolder.username, fileLength, expiration, deleteAfterNumberDownloads, digestType, passwordHexDigest, fileIsPrivate), accessorHolder, messageBasedFactory, properties);
        if (!reply.isOk())
            throw new FiletransferException(reply.getException());

        String filename = file.getName();
        chunkLength = reply.getChunkLength();
        DigestInputStream digestInputStream = new DigestInputStream(new FileInputStream(file), MessageDigest.getInstance(digestType));
        BufferedInputStream in = new BufferedInputStream(digestInputStream);
        byte[] chunk = new byte[chunkLength];
        int len = 0;
        long cumulatedLen = 0;
        int chunkNo = 1;
        while ((len = in.read(chunk, 0, Math.min(chunkLength, (int) (fileLength - cumulatedLen)))) != -1) {
            link = null;
            cumulatedLen += len;
            boolean last = cumulatedLen == fileLength;
            boolean replyRequired = (chunkNo % replyInterval == 0) || last;
            FileChunkReply chunkReply = (FileChunkReply) request(new FileChunkRequest(replyRequired, chunkNo, last, chunk, len), accessorHolder, messageBasedFactory);
            if (replyRequired) {
                if (chunkReply.isOk()) {
                    if (chunkReply.getChunkNo() != chunkNo)
                        throw new FiletransferException("Chunks no mismatch, sent: " + chunkNo + ", received: " + chunkReply.getChunkNo());
                    if (last) {
                        String linkString = chunkReply.getLink();
                        if (linkString == null)
                            throw new FiletransferException("No link returned on the last chunk");
                        link = linkString;
                    }
                } else
                    throw new FiletransferException(reply.getException());
            }
            if (progressListener != null && replyRequired)
                progressListener.progress(filename, chunkNo, fileLength, cumulatedLen, (int) (cumulatedLen * 100 / fileLength));
            chunkNo++;
            if (last)
                break;
        }
        byte[] cacheDigest = new LinkParser(link).getDigest();
        byte[] localDigest = digestInputStream.getMessageDigest().digest();
        if (cacheDigest == null || localDigest == null || !Arrays.equals(localDigest, cacheDigest))
            throw new FiletransferException("Cache digest is not equal to local digest");
        return link;
    }

    public Filetransfer receive() throws FiletransferException, JMSException, IOException, NoSuchAlgorithmException {
        return receive(null);
    }

    public Filetransfer receive(ProgressListener progressListener) throws FiletransferException, JMSException, IOException, NoSuchAlgorithmException {
        checkState();
        if (!useOriginalFilename && file == null)
            throw new NullPointerException("File is not specified");
        if (useOriginalFilename && outputDir == null)
            throw new NullPointerException("Output directory is not specified");
        if (link == null)
            throw new NullPointerException("Link is not specified");

        LinkParser linkParser = new LinkParser(link);
        String pwdHexDigest = null;
        if (password != null) {
            byte[] pwdigest = MessageDigest.getInstance(linkParser.getDigestType()).digest(password.getBytes());
            pwdHexDigest = Util.byteArrayToHexString(pwdigest);
        }
        FileConsumeReply reply = (FileConsumeReply) request(new FileConsumeRequest(accessorHolder.replyQueue.getQueueName(), link, pwdHexDigest, replyInterval), accessorHolder, messageBasedFactory, null);
        if (!reply.isOk())
            throw new FiletransferException(reply.getException());

        if (useOriginalFilename)
            file = new File(outputDir, reply.getFilename());
        if (file.exists())
            file.delete();
        int currentInboundChunkNo = 0;
        long cumulatedLen = 0;
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        byte[] localDigest = null;
        DigestOutputStream digestOutputStream = new DigestOutputStream(new BufferedOutputStream(fileOutputStream, reply.getChunkLength()), MessageDigest.getInstance(linkParser.getDigestType()));
        try {
            for (; ; ) {
                Message message = accessorHolder.consumer.receive(RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT);
                if (message == null)
                    throw new JMSException("Request timeout occured (" + RequestRegistry.SWIFTMQ_REQUEST_TIMEOUT + ") ms");
                FileChunkRequest chunkRequest = (FileChunkRequest) messageBasedFactory.create(message);
                if (chunkRequest.getChunkNo() != currentInboundChunkNo + 1)
                    throw new FiletransferException("ChunkNo out of order! Expecting " + (currentInboundChunkNo + 1) + " but was receiving " + chunkRequest.getChunkNo());
                if (chunkRequest != null) {
                    byte[] chunk = chunkRequest.getChunk();
                    digestOutputStream.write(chunk);
                    digestOutputStream.flush();
                    cumulatedLen += chunk.length;
                    FileChunkReply chunkReply = null;
                    if (chunkRequest.isReplyRequired())
                        chunkReply = (FileChunkReply) chunkRequest.createReplyInstance();
                    if (chunkReply != null) {
                        chunkReply.setOk(true);
                        chunkReply.setChunkNo(chunkRequest.getChunkNo());
                        if (progressListener != null)
                            progressListener.progress(reply.getFilename(), chunkRequest.getChunkNo(), reply.getSize(), cumulatedLen, (int) (cumulatedLen * 100 / reply.getSize()));
                    }
                    currentInboundChunkNo = chunkRequest.getChunkNo();
                    if (chunkRequest.isLast()) {
                        fileOutputStream.getFD().sync();
                        localDigest = digestOutputStream.getMessageDigest().digest();
                        digestOutputStream.close();
                        byte[] cacheDigest = new LinkParser(link).getDigest();
                        if (!Arrays.equals(localDigest, cacheDigest)) {
                            chunkReply.setOk(false);
                            chunkReply.setException("Cache digest is not equal to local digest");
                            accessorHolder.producer.send(chunkReply.toMessage());
                            throw new FiletransferException("Cache digest is not equal to local digest");
                        } else {
                            chunkReply.setLink(new LinkBuilder(linkParser.getRouterName(), linkParser.getCacheName(), linkParser.getFileKey(), linkParser.getDigestType(), localDigest).toString());
                            accessorHolder.producer.send(chunkReply.toMessage());
                        }
                        break;
                    } else {
                        if (chunkReply != null)
                            accessorHolder.producer.send(chunkReply.toMessage());
                    }
                }
            }
        } catch (JMSException e) {
            if (file.exists())
                file.delete();
            throw e;
        } catch (FiletransferException e) {
            if (file.exists())
                file.delete();
            throw e;
        } catch (IOException e) {
            if (file.exists())
                file.delete();
            throw e;
        }

        return this;
    }

    public Filetransfer delete() throws FiletransferException, JMSException, IOException, NoSuchAlgorithmException {
        checkState();
        if (link == null)
            throw new NullPointerException("Link is not specified");
        LinkParser linkParser = new LinkParser(link);
        String pwdHexDigest = null;
        if (password != null) {
            byte[] pwdigest = MessageDigest.getInstance(linkParser.getDigestType()).digest(password.getBytes());
            pwdHexDigest = Util.byteArrayToHexString(pwdigest);
        }
        FileDeleteReply reply = (FileDeleteReply) request(new FileDeleteRequest(link, pwdHexDigest), accessorHolder, messageBasedFactory, null);
        if (!reply.isOk())
            throw new FiletransferException(reply.getException());
        return this;
    }

    public List<String> query() throws Exception {
        checkState();
        FileQueryReply reply = (FileQueryReply) request(new FileQueryRequest(selector), accessorHolder, messageBasedFactory, null);
        if (!reply.isOk())
            throw new FiletransferException(reply.getException());
        return reply.getResult();
    }

    public Map<String, Map<String, Object>> queryProperties() throws Exception {
        checkState();
        FileQueryPropsReply reply = (FileQueryPropsReply) request(new FileQueryPropsRequest(link, selector), accessorHolder, messageBasedFactory, null);
        if (!reply.isOk())
            throw new FiletransferException(reply.getException());
        return reply.getResult();
    }

    public void close() {
        try {
            request(new SessionCloseRequest(), accessorHolder, messageBasedFactory);
        } catch (Exception e) {
        }
        try {
            accessorHolder.producer.close();
        } catch (JMSException e) {
        }
        try {
            accessorHolder.consumer.close();
        } catch (JMSException e) {
        }
        try {
            accessorHolder.session.close();
        } catch (JMSException e) {
        }
        try {
            accessorHolder.replyQueue.delete();
        } catch (JMSException e) {
        }
        closed = true;
    }
}
