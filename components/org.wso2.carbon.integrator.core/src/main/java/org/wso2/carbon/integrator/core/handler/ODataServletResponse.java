/*
 * Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.integrator.core.handler;

import org.apache.axis2.context.MessageContext;
import org.apache.http.protocol.HTTP;
import org.apache.synapse.SynapseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

/**
 * This class builds a Servlet response from a message context.
 *
 */
public class ODataServletResponse implements HttpServletResponse {

    private final static int OUTPUT_STREAM_BUFFER_SIZE = 4096;
    public final ByteArrayOutputStream content = new ByteArrayOutputStream(OUTPUT_STREAM_BUFFER_SIZE);
    private final ResponseServletOutputStream outputStream = new ResponseServletOutputStream(this.content);
    public MessageContext axis2MessageContext;
    private boolean stopStream = false;
    private long contentLength = 0;
    private String contentType;
    private String characterEncoding = StandardCharsets.UTF_8.name();
    private int bufferSize = OUTPUT_STREAM_BUFFER_SIZE;
    private int statusCode;
    private boolean committed;

    public ODataServletResponse(MessageContext axis2MessageContext) {
        this.axis2MessageContext = axis2MessageContext;
    }

    /**
     * This method will signal when to start streaming.
     *
     * @return
     */
    public boolean startStream() {
        return this.outputStream.startStream;
    }

    /**
     * This method will signal when to stop streaming.
     *
     * @return
     */
    public boolean stopStream() {
        return this.stopStream;
    }

    @Override
    public void addCookie(Cookie cookie) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsHeader(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeURL(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectURL(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeUrl(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectUrl(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendError(int i, String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendError(int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendRedirect(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDateHeader(String s, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDateHeader(String s, long l) {
        throw new UnsupportedOperationException();
    }

    public void setContentLengthLong(long l) {
        throw new UnsupportedOperationException();
    }

    public void setWriteListener(javax.servlet.WriteListener l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHeader(String headerName, String headerValue) {

    }

    /**
     * This method sets content type and content length parameters.
     *
     * @param headerName
     * @param headerValue
     */
    @Override
    public void addHeader(String headerName, String headerValue) {
        if (HTTP.CONTENT_TYPE.equals(headerName)) {
            contentType = headerValue;
        }
        if (HTTP.CONTENT_LEN.equals(headerName)) {
            contentLength = Long.parseLong(headerValue);
        }
    }

    @Override
    public void setIntHeader(String s, int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addIntHeader(String s, int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStatus(int i, String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStatus() {
        return statusCode;
    }

    @Override
    public void setStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    public String getHeader(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> getHeaders(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> getHeaderNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCharacterEncoding() {
        return characterEncoding;
    }

    @Override
    public void setCharacterEncoding(String s) {
        this.characterEncoding = characterEncoding;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public void setContentType(String s) {
        contentType = s;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        return this.outputStream;
    }

    /**
     * This method sets committed to true if the buffer size is exceeded.
     *
     */
    private void setCommittedIfBufferSizeExceeded() {
        int bufSize = getBufferSize();
        if (bufSize > 0 && this.content.size() > bufSize) {
            setCommitted(true);
        }
    }

    /**
     * This method flushes the remaining bytes to the output stream and close the response.
     *
     */
    public void close() {
        try {
            this.outputStream.flushByteArray();
            this.content.close();
            this.outputStream.close();
        } catch (IOException e) {
            throw new SynapseException("Error occurred while trying to close the Servlet response.", e);
        }
    }

    @Override
    public PrintWriter getWriter() {
        throw new UnsupportedOperationException();
    }

    public int getContentLength() {
        return (int) this.contentLength;
    }

    @Override
    public void setContentLength(int i) {
        contentLength = i;
    }

    /**
     * This method returns the content in the output stream in string format.
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String getContentAsString() throws IOException {
        if (this.content != null) {
            return outputStream.readOutputStream(this.characterEncoding);
        }
        return "";
    }

    @Override
    public int getBufferSize() {
        return this.bufferSize;
    }

    @Override
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public void flushBuffer() {
        setCommitted(true);
    }

    @Override
    public void resetBuffer() {
        this.content.reset();
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    @Override
    public void reset() {
        resetBuffer();
        this.characterEncoding = null;
        this.contentLength = 0;
        this.contentType = null;
    }

    @Override
    public Locale getLocale() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLocale(Locale locale) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method will set stopStream to true. Forcing the response to be closed.
     *
     */
    public void endStream() {
        this.stopStream = true;
    }

    private class ResponseServletOutputStream extends ServletOutputStream {

        public boolean readWithoutLimit = false;
        private boolean startStream = false;
        private OutputStream targetStream;
        private byte[] byteArr = new byte[OUTPUT_STREAM_BUFFER_SIZE];
        private int byteCount = 0;

        ResponseServletOutputStream(OutputStream out) {
            targetStream = out;
        }

        /**
         * This method writes a byte to a byte array. Once the byte array is full the entire array is written to the
         * output stream.
         * This will reduce the latency of the response.
         *
         * @param b
         * @throws IOException
         */
        @Override
        public void write(int b) throws IOException {
            byteArr[byteCount++] = (byte) b;
            if (!startStream) {
                startStream = true;
            }
            if (byteCount >= OUTPUT_STREAM_BUFFER_SIZE) {
                writeOutputStream();
                byteCount = 0;
            }
        }

        /**
         * This method writes a byte array to the output stream.
         *
         * @throws IOException
         */
        private synchronized void writeOutputStream() throws IOException {
            targetStream.write(byteArr);
            super.flush();
            targetStream.flush();
            setCommittedIfBufferSizeExceeded();
            byteCount = 0;
        }

        /**
         * This method returns the content in the output stream in string format.
         *
         * @throws IOException
         */
        private synchronized String readOutputStream(String characterEncoding) throws IOException {
            String response = "";
            if (this.readWithoutLimit || ((ByteArrayOutputStream) targetStream).size() > OUTPUT_STREAM_BUFFER_SIZE * 8) {
                response = (characterEncoding != null ? ((ByteArrayOutputStream) targetStream).toString(
                        characterEncoding) : ((ByteArrayOutputStream) targetStream).toString());
                ((ByteArrayOutputStream) targetStream).reset();
            }
            if (this.readWithoutLimit) {
                stopStream = true;
            }
            return response;
        }

        /**
         * This method reads the remaining content in the output stream (content that has not yet read).
         *
         * @throws IOException
         */
        private synchronized void flushByteArray() throws IOException {
            if (byteCount < OUTPUT_STREAM_BUFFER_SIZE) {
                targetStream.write(byteArr, 0, byteCount);
                super.flush();
                targetStream.flush();
                setCommittedIfBufferSizeExceeded();
                byteCount = 0;
                byteArr = new byte[OUTPUT_STREAM_BUFFER_SIZE];
            }
            readWithoutLimit = true;
        }

        @Override
        public void flush() throws IOException {
            super.flush();
            setCommitted(true);
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.targetStream.close();
        }
    }
}
