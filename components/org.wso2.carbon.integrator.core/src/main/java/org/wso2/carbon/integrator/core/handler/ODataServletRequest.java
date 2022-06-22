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

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.collections4.iterators.IteratorEnumeration;
import org.apache.synapse.commons.json.JsonUtil;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.Part;

/**
 * This class builds a Servlet request from a message context.
 *
 */
public class ODataServletRequest implements HttpServletRequest {

    private MessageContext axis2MessageContext;

    public ODataServletRequest(MessageContext axis2MessageContext) {
        this.axis2MessageContext = axis2MessageContext;
    }

    @Override
    public String getAuthType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cookie[] getCookies() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDateHeader(String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method returns a transport header.
     *
     * @param s
     * @return
     */
    @Override
    public String getHeader(String s) {
        Map transportHeaders = (Map) axis2MessageContext.getProperty(MessageContext.TRANSPORT_HEADERS);
        return (String) transportHeaders.get(s);
    }

    /**
     * This method return a list of transport headers.
     *
     * @param s
     * @return
     */
    @Override
    public Enumeration getHeaders(String s) {
        Map transportHeaders = (Map) axis2MessageContext.getProperty(MessageContext.TRANSPORT_HEADERS);
        List headerValues = new ArrayList();
        headerValues.add(transportHeaders.get(s));
        return new IteratorEnumeration(headerValues.iterator());
    }

    /**
     * This method returns a list of transport header names.
     *
     * @return
     */
    @Override
    public Enumeration getHeaderNames() {
        Map transportHeaders = (Map) axis2MessageContext.getProperty(MessageContext.TRANSPORT_HEADERS);
        return new IteratorEnumeration(transportHeaders.keySet().iterator());
    }

    @Override
    public int getIntHeader(String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method returns the HTTP method.
     *
     * @return
     */
    @Override
    public String getMethod() {
        return (String) axis2MessageContext.getProperty("HTTP_METHOD");
    }

    @Override
    public String getPathInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPathTranslated() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method returns the context path of the request.
     * For Odata requests this will always be "/odata"
     *
     * @return
     */
    @Override
    public String getContextPath() {
        return "/odata";
    }

    /**
     * This method returns the OData query part of the request.
     *
     * @return
     */
    @Override
    public String getQueryString() {
        String queryString = null;
        String url = getRequestURI();
        int queryStringPosition = url.indexOf('?');
        if (queryStringPosition != -1) {
            queryString = url.substring(queryStringPosition + 1);
        }
        return queryString;
    }

    @Override
    public String getRemoteUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUserInRole(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Principal getUserPrincipal() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRequestedSessionId() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method returns the request URI.
     *
     * @return
     */
    public String getRequestURI() {
        return axis2MessageContext.getProperty("TransportInURL").toString();
    }

    /**
     * This method returns the request URL.
     *
     * @return
     */
    public StringBuffer getRequestURL() {
        StringBuffer url = new StringBuffer();
        url.append(axis2MessageContext.getProperty("SERVICE_PREFIX"));
        String serviceURL = (String) axis2MessageContext.getProperty("TransportInURL");
        int queryStringPosition = serviceURL.indexOf('?');
        if (queryStringPosition != -1) {
            url.append(serviceURL, 1, serviceURL.indexOf('?'));
        } else {
            url.append(serviceURL.substring(1));
        }
        return url;
    }

    /**
     * This method returns the servlet path.
     * For odata requests this will always be empty.
     *
     * @return
     */
    @Override
    public String getServletPath() {
        return "";
    }

    @Override
    public HttpSession getSession(boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpSession getSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method checks if authentication is required.
     * For Odata requests authentication is not required.
     *
     * @param httpServletResponse
     * @return
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public boolean authenticate(HttpServletResponse httpServletResponse) throws IOException, ServletException {
        return false;
    }

    @Override
    public void login(String s, String s1) throws ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void logout() throws ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Part getPart(String s) throws IOException, ServletException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String s) {
        return null;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCharacterEncoding() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterEncoding(String s) throws UnsupportedEncodingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getContentLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getContentType() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method returns an input stream to write content to the message context.
     *
     * @return
     * @throws IOException
     */
    @Override
    public ServletInputStream getInputStream() throws IOException {
        OMElement content = axis2MessageContext.getEnvelope().getBody().getFirstElement();
        if (content != null) {
            String contentType = (String) axis2MessageContext.getProperty(Constants.Configuration.CONTENT_TYPE);
            if (contentType == null) {
                return new RequestServletInputStream(new ByteArrayInputStream(content.toString().getBytes()));
            } else if (contentType.contains("application/json")) {
                ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                JsonUtil.writeAsJson(axis2MessageContext, byteOutputStream);
                return new RequestServletInputStream(new ByteArrayInputStream(byteOutputStream.toByteArray()));
            } else if (contentType.contains("application/xml")) {
                return new RequestServletInputStream(new ByteArrayInputStream(content.toString().getBytes()));
            } else {
                return new RequestServletInputStream(new ByteArrayInputStream(content.toString().getBytes()));
            }
        }
        return null;
    }

    @Override
    public String getParameter(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumeration<String> getParameterNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getParameterValues(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProtocol() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getScheme() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServerName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getServerPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferedReader getReader() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRemoteAddr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRemoteHost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String s, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAttribute(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Locale getLocale() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumeration<Locale> getLocales() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRealPath(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRemotePort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLocalName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLocalAddr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServletContext getServletContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
            throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    /**
     * For OData request asynchronous operations are not supported.
     *
     * @return
     */
    @Override
    public boolean isAsyncStarted() {
        return false;
    }

    /**
     * Checks if the request supports asynchronous operation.
     * For OData request asynchronous operations are not supported.
     *
     * @return
     */
    @Override
    public boolean isAsyncSupported() {
        return false;
    }

    @Override
    public AsyncContext getAsyncContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DispatcherType getDispatcherType() {
        throw new UnsupportedOperationException();
    }

    private class RequestServletInputStream extends ServletInputStream {

        InputStream sourceStream;

        public RequestServletInputStream(InputStream sourceStream) {
            this.sourceStream = sourceStream;
        }

        @Override
        public int read() throws IOException {
            return this.sourceStream.read();
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.sourceStream.close();
        }
    }

}
