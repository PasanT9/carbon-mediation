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

import java.io.IOException;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Objects;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.TransportSender;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.commons.io.IOUtils;
import org.apache.http.nio.NHttpServerConnection;
import org.apache.synapse.SynapseException;
import org.apache.synapse.transport.nhttp.util.MessageFormatterDecoratorFactory;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.PassThroughHttpSender;
import org.apache.synapse.transport.passthru.Pipe;
import org.apache.synapse.transport.passthru.ProtocolState;
import org.apache.synapse.transport.passthru.ServerWorker;
import org.apache.synapse.transport.passthru.SourceContext;
import org.apache.synapse.transport.passthru.SourceRequest;
import org.apache.synapse.transport.passthru.SourceResponse;
import org.apache.synapse.transport.passthru.config.SourceConfiguration;
import org.apache.synapse.transport.passthru.util.PassThroughTransportUtils;
import org.apache.synapse.transport.passthru.util.RelayUtils;
import org.apache.synapse.transport.passthru.util.SourceResponseFactory;
import org.wso2.caching.CachingConstants;
import org.wso2.caching.digest.DigestGenerator;
import org.wso2.carbon.integrator.core.Utils;

/**
 * This class streams a SOAP message to the client given a response that is also streaming.
 *
 */
public class ODataTransportSender extends PassThroughHttpSender implements TransportSender {

    public DigestGenerator digestGenerator;
    private ODataServletResponse response;

    public ODataTransportSender(TransportSender sender, ConfigurationContext configurationContext,
                                TransportOutDescription transportOut, ODataServletResponse response) {
        this.digestGenerator = CachingConstants.DEFAULT_XML_IDENTIFIER;
        this.response = response;
        try {
            init(configurationContext, transportOut);
        } catch (AxisFault e) {
            throw new SynapseException("Error occurred while initializing the Stream Sender.", e);
        }
    }

    /**
     * This method builds a SOAP message from a streaming Servlet response and stream the message back to the client.
     *
     * @param msgContext
     * @throws IOException
     */
    public void submitResponse(MessageContext msgContext) throws IOException {
        SourceConfiguration sourceConfiguration = (SourceConfiguration) msgContext.getProperty(
                "PASS_THROUGH_SOURCE_CONFIGURATION");
        NHttpServerConnection conn = (NHttpServerConnection) msgContext.getProperty("pass-through.Source-Connection");
        if (conn == null) {
            StreamWorker serverWorker = (StreamWorker) msgContext.getProperty("OutTransportInfo");
            if (serverWorker == null) {
                throw new IllegalStateException("Unable to correlate the response to a request.");
            }
            MessageContext requestContext = serverWorker.getRequestContext();
            conn = (NHttpServerConnection) requestContext.getProperty("pass-through.Source-Connection");
            sourceConfiguration = (SourceConfiguration) requestContext.getProperty("PASS_THROUGH_SOURCE_CONFIGURATION");
        }
        if (msgContext.getProperty("HTTP_ETAG") != null && (Boolean) msgContext.getProperty("HTTP_ETAG")) {
            try {
                RelayUtils.buildMessage(msgContext);
            } catch (IOException | XMLStreamException e) {
                throw new SynapseException("Error occurred while building the message.", e);
            }
            String hash = this.digestGenerator.getDigest(msgContext);
            Map headers = (Map) msgContext.getProperty("TRANSPORT_HEADERS");
            headers.put("ETag", "\"" + hash + "\"");
        }
        if (msgContext.getProperty("enableMTOM") != null && !Boolean.TRUE.equals(
                msgContext.getProperty("message.builder.invoked"))) {
            try {
                RelayUtils.buildMessage(msgContext);
            } catch (IOException | XMLStreamException e) {
                throw new SynapseException("Error occurred while building the message.", e);
            }
        }
        SourceRequest sourceRequest = SourceContext.getRequest(conn);
        if (sourceRequest == null) {
            if (conn.getContext().getAttribute("SOURCE_CONNECTION_DROPPED") == null || !(Boolean) conn.getContext()
                    .getAttribute("SOURCE_CONNECTION_DROPPED")) {
                this.log.warn("Trying to submit a response to an already closed connection : " + conn);
            }
        } else {
            SourceResponse sourceResponse = SourceResponseFactory.create(msgContext, sourceRequest,
                                                                         sourceConfiguration);
            sourceResponse.checkResponseChunkDisable(msgContext);
            conn.getContext().setAttribute("RESPONSE_MESSAGE_CONTEXT", msgContext);
            SourceContext.setResponse(conn, sourceResponse);
            Boolean noEntityBody = (Boolean) msgContext.getProperty("NO_ENTITY_BODY");
            Pipe pipe = (Pipe) msgContext.getProperty("pass-through.pipe");
            if (noEntityBody == null || !noEntityBody || pipe != null) {
                if (pipe == null) {
                    pipe = new Pipe(sourceConfiguration.getBufferFactory().getBuffer(), "Test", sourceConfiguration);
                    msgContext.setProperty("pass-through.pipe", pipe);
                    msgContext.setProperty("message.builder.invoked", Boolean.TRUE);
                }
                pipe.attachConsumer(conn);
                sourceResponse.connect(pipe);
            }
            Integer errorCode = (Integer) msgContext.getProperty("ERROR_CODE");
            if (errorCode != null) {
                sourceResponse.setStatus(502);
                SourceContext.get(conn).setShutDown(true);
            }
            ProtocolState state = SourceContext.getState(conn);
            if (state != null && state.compareTo(ProtocolState.REQUEST_DONE) <= 0) {
                boolean noEntityBodyResponse = false;
                OutputStream out;
                if (noEntityBody != null && Boolean.TRUE == noEntityBody && pipe != null) {
                    out = pipe.getOutputStream();
                    out.write(new byte[0]);
                    pipe.setRawSerializationComplete(true);
                    out.close();
                    noEntityBodyResponse = true;
                }
                if (!noEntityBodyResponse && msgContext.isPropertyTrue("message.builder.invoked") && pipe != null) {
                    out = pipe.getOutputStream();
                    if ("true".equals(msgContext.getProperty("enableMTOM")) || "true".equals(
                            msgContext.getProperty("enableSwA"))) {
                        Object contentType = msgContext.getProperty("ContentType");
                        if (Objects.isNull(contentType) || !((String) contentType).trim().startsWith(
                                "multipart/related")) {
                            msgContext.setProperty("ContentType", "multipart/related");
                        }
                        msgContext.setProperty("messageType", "multipart/related");
                    }
                    MessageFormatter formatter = MessageFormatterDecoratorFactory.createMessageFormatterDecorator(
                            msgContext);
                    OMOutputFormat format = PassThroughTransportUtils.getOMOutputFormat(msgContext);
                    boolean initResponseComplete = false;
                    while (!response.stopStream()) {
                        try {
                            if (setContent(msgContext, response)) {
                                if (!initResponseComplete) {
                                    initResponse(msgContext, response, formatter, format, sourceResponse);
                                    initResponseComplete = true;
                                }
                                formatter.writeTo(msgContext, format, out, false);
                            }
                        } catch (RemoteException | XMLStreamException | InterruptedException e) {
                            IOUtils.closeQuietly(out);
                            throw new SynapseException("Error occurred while building the message context.", e);
                        }
                    }
                    pipe.setSerializationComplete(true);
                    out.close();
                    response.close();
                }
                conn.requestOutput();
            } else {
                if (errorCode != null) {
                    if (this.log.isDebugEnabled()) {
                        this.log.warn("A Source connection is closed because of an error in target: " + conn);
                    }
                } else {
                    this.log.debug(
                            "A Source Connection is closed, because source handler is already in the process of writing a response while another response is submitted: "
                                    + conn);
                }
                pipe.consumerError();
                SourceContext.updateState(conn, ProtocolState.CLOSED);
                sourceConfiguration.getSourceConnections().shutDownConnection(conn, true);
            }
        }
    }

    /**
     * This method sets the content type and the status code of the source response.
     *
     * @param msgContext
     * @param response
     * @param formatter
     * @param format
     * @param sourceResponse
     */
    private void initResponse(MessageContext msgContext, ODataServletResponse response, MessageFormatter formatter,
                              OMOutputFormat format, SourceResponse sourceResponse) {
        msgContext.setProperty(PassThroughConstants.HTTP_SC, response.getStatus());
        if (response.getContentType() != null && response.getContentType().contains(Utils.JSON_CONTENT_TYPE)) {
            msgContext.setProperty("ContentType", Utils.JSON_CONTENT_TYPE);
        } else if (response.getContentType() != null && response.getContentType().contains(Utils.XML_CONTENT_TYPE)) {
            msgContext.setProperty("ContentType", Utils.XML_CONTENT_TYPE);
        }
        this.setContentType(msgContext, sourceResponse, formatter, format);
        sourceResponse.setStatus(response.getStatus());
    }

    /**
     * This method builds the SOAP envelope according to the state of the response.
     *
     * @param axis2MessageContext
     * @param response
     * @return
     * @throws IOException
     * @throws XMLStreamException
     * @throws InterruptedException
     */
    private boolean setContent(org.apache.axis2.context.MessageContext axis2MessageContext,
                               ODataServletResponse response)
            throws IOException, XMLStreamException, InterruptedException {
        if (isInvalidResponse(response) && !response.startStream()) {
            setMessageEnvelope(axis2MessageContext, "");
            response.endStream();
            return true;
        }
        String content = response.getContentAsString();
        if (response.startStream() && !content.equals("")) {
            setMessageEnvelope(axis2MessageContext, content);
            return true;
        }
        return false;
    }

    /**
     * This method builds the SOAP envelope using the given string content.
     *
     * @param axis2MessageContext
     * @param content
     * @throws AxisFault
     */
    private void setMessageEnvelope(MessageContext axis2MessageContext, String content) throws AxisFault {
        SOAPFactory fac = OMAbstractFactory.getSOAP11Factory();
        SOAPEnvelope envelope = fac.getDefaultEnvelope();
        envelope.getBody().addChild(getTextElement(content));
        axis2MessageContext.setEnvelope(envelope);
    }

    /**
     * Checks for unsuccessful responses or responses without a body.
     *
     * @param response
     * @return
     */
    private boolean isInvalidResponse(ODataServletResponse response) {
        int statusCode = response.getStatus();
        if (statusCode == 0 || ((statusCode >= 200 && statusCode < 300) && statusCode != Utils.NO_CONTENT)) {
            return false;
        }
        return true;
    }

    /**
     * This method builds an OMElement using the given text.
     *
     * @param content
     * @return
     */
    private OMElement getTextElement(String content) {
        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMElement textElement = factory.createOMElement(new QName("http://ws.apache.org/commons/ns/payload", "text"));
        if (content == null) {
            content = "";
        }
        textElement.setText(content);
        return textElement;
    }

    /**
     * This method sets the content type headers of the source response.
     *
     * @param msgContext
     * @param sourceResponse
     * @param formatter
     * @param format
     */
    public void setContentType(MessageContext msgContext, SourceResponse sourceResponse, MessageFormatter formatter,
                               OMOutputFormat format) {
        Object contentTypeInMsgCtx = msgContext.getProperty("ContentType");
        boolean isContentTypeSetFromMsgCtx = false;
        if (contentTypeInMsgCtx != null) {
            String contentTypeValueInMsgCtx = contentTypeInMsgCtx.toString();
            if (!contentTypeValueInMsgCtx.contains("multipart/related") && !contentTypeValueInMsgCtx.contains(
                    "multipart/form-data")) {
                if (format != null && contentTypeValueInMsgCtx.indexOf(HTTPConstants.CHAR_SET_ENCODING) == -1
                        && !"false".equals(msgContext.getProperty("setCharacterEncoding"))) {
                    String encoding = format.getCharSetEncoding();
                    if (encoding != null) {
                        contentTypeValueInMsgCtx = contentTypeValueInMsgCtx + "; charset=" + encoding;
                    }
                }
                sourceResponse.removeHeader("Content-Type");
                sourceResponse.addHeader("Content-Type", contentTypeValueInMsgCtx);
                isContentTypeSetFromMsgCtx = true;
            }
        }
        if (!isContentTypeSetFromMsgCtx) {
            sourceResponse.removeHeader("Content-Type");
            sourceResponse.addHeader("Content-Type",
                                     formatter.getContentType(msgContext, format, msgContext.getSoapAction()));
        }
    }

    /**
     * This class processes an incoming request through Axis2.
     *
     */
    public class StreamWorker extends ServerWorker {
        private MessageContext msgContext;

        public StreamWorker(SourceRequest request, SourceConfiguration sourceConfiguration, OutputStream os) {
            super(request, sourceConfiguration, os);
        }

        protected MessageContext getRequestContext() {
            return this.msgContext;
        }
    }
}
