/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.apache.axis2.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.protocol.HTTP;
import org.apache.synapse.AbstractSynapseHandler;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.builtin.SendMediator;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.core.PassThroughSenderManager;
import org.apache.synapse.transport.passthru.util.RelayUtils;
import org.wso2.carbon.dataservices.core.odata.ODataServiceHandler;
import org.wso2.carbon.dataservices.core.odata.ODataServiceRegistry;
import org.wso2.carbon.integrator.core.Utils;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamException;

/**
 * This handler is written to process incoming messages to the passthrough endpoint.
 *
 */
public class IntegratorSynapseHandler extends AbstractSynapseHandler {

    private static final Log log = LogFactory.getLog(IntegratorSynapseHandler.class);

    public IntegratorSynapseHandler() {
        this.sendMediator = new SendMediator();
    }

    private SendMediator sendMediator;
    private PassThroughSenderManager passThroughSenderManager = PassThroughSenderManager.getInstance();

    private static final String MESSAGE_DISPATCHED = "MessageDispatched";
    private static final String RESPONSE_WRITTEN = "RESPONSE_WRITTEN";
    private static final String SUPER_TENANT_DOMAIN_NAME = "carbon.super";
    private static final int NOT_IMPLEMENTED = 501;

    @Override
    public boolean handleRequestInFlow(MessageContext messageContext) {
        boolean isPreserveHeadersContained = false;
        try {
            org.apache.axis2.context.MessageContext axis2MessageContext =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext();
            Object isODataService = axis2MessageContext.getProperty("IsODataService");
            // In this if block we are skipping proxy services, inbound related message contexts & api.
            if (axis2MessageContext.getProperty("TransportInURL") != null && isODataService != null) {
                RelayUtils.buildMessage(axis2MessageContext);
                ODataServletRequest request = new ODataServletRequest(axis2MessageContext);
                ODataServletResponse response = new ODataServletResponse(axis2MessageContext);
                initProcess(request, response);
                streamResponseBack(response, messageContext);
            }
            return true;
        } catch (Exception e) {
            this.handleException("Error occurred in integrator handler.", e, messageContext);
            return true;
        }
        finally {
            if (isPreserveHeadersContained) {
                if (passThroughSenderManager != null &&
                        passThroughSenderManager.getSharedPassThroughHttpSender() != null) {
                    try {
                        passThroughSenderManager.getSharedPassThroughHttpSender()
                                .removePreserveHttpHeader(HTTP.USER_AGENT);
                        // This catch is added when there is no preserve headers in the PassthoughHttpSender.
                    } catch (ArrayIndexOutOfBoundsException e) {
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "ArrayIndexOutOfBoundsException exception occurred, when removing preserve " +
                                            "headers.");
                        }
                    }
                }
            }
        }
    }

    /**
     * This method initiates processing the servlet request.
     *
     * @param request
     * @param response
     */
    private void initProcess(ODataServletRequest request, ODataServletResponse response) {
        String[] serviceParams = request.getRequestURI().split("/odata/")[1].split("/");
        String domain = "";
        String serviceRootPath = "";
        String serviceKey = "";
        if (serviceParams[0].equals("t")) {
            domain = serviceParams[1];
            serviceRootPath = "/" + serviceParams[2] + "/" + serviceParams[3];
            serviceKey = serviceParams[2] + serviceParams[3];
        } else {
            domain = SUPER_TENANT_DOMAIN_NAME;
            serviceRootPath = "/" + serviceParams[0] + "/" + serviceParams[1];
            serviceKey = serviceParams[0] + serviceParams[1];
        }
        ODataServiceRegistry registry = ODataServiceRegistry.getInstance();
        ODataServiceHandler oDataServiceHandler = registry.getServiceHandler(serviceKey, domain);
        processServletRequest(request, response, serviceRootPath, oDataServiceHandler);
    }

    /**
     * This method will process the servlet request and builds the response.
     *
     * @param request
     * @param response
     * @param serviceRootPath
     * @param oDataServiceHandler
     * @return
     */
    private Thread processServletRequest(ODataServletRequest request, ODataServletResponse response,
                                         String serviceRootPath, ODataServiceHandler oDataServiceHandler) {
        Thread streamThread = null;
        if (oDataServiceHandler != null) {
            streamThread = new Thread() {
                @Override
                public void run() {
                    try {
                        oDataServiceHandler.process(request, response, serviceRootPath);
                    } catch (Exception e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to process the servlet request.");
                        }
                        throw new SynapseException("Error occurred while processing the request.", e);
                    } finally {
                        response.close();
                    }
                }
            };
            streamThread.start();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Couldn't find the ODataService Handler for " + serviceRootPath + " Service.");
            }
            response.setStatus(NOT_IMPLEMENTED);
            response.close();
        }
        return streamThread;
    }

    /**
     * This method starts streaming the response from the server to the client.
     *
     * @param response
     * @param messageContext
     * @throws XMLStreamException
     * @throws IOException
     */
    private void streamResponseBack(ODataServletResponse response, MessageContext messageContext)
            throws XMLStreamException, IOException {
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        axis2MessageContext.setProperty(Constants.Configuration.MESSAGE_TYPE, Utils.TEXT_CONTENT_TYPE);
        axis2MessageContext.removeProperty(PassThroughConstants.NO_ENTITY_BODY);
        RelayUtils.buildMessage(axis2MessageContext);
        messageContext.setTo(null);
        messageContext.setResponse(true);
        axis2MessageContext.getOperationContext().setProperty(RESPONSE_WRITTEN, "SKIP");
        ODataAxisEngine.stream(axis2MessageContext, response);
    }

    @Override
    public boolean handleRequestOutFlow(MessageContext messageContext) {
        return false;
    }

    @Override
    public boolean handleResponseInFlow(MessageContext messageContext) {

        if ("true".equals(messageContext.getProperty(MESSAGE_DISPATCHED))) {
            //remove the "MessageDispatched" property
            Set keySet = messageContext.getPropertyKeySet();
            keySet.remove(MESSAGE_DISPATCHED);

            // In here, We are rewriting the location header which comes from the particular registered endpoints.
            Object headers =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext().getProperty("TRANSPORT_HEADERS");
            if (headers instanceof TreeMap) {
                String locationHeader = (String) ((TreeMap) headers).get("Location");
                if (locationHeader != null) {
                    Utils.rewriteLocationHeader(locationHeader, messageContext);
                }
            }
            messageContext.setTo(null);
            messageContext.setResponse(true);
            Axis2MessageContext axis2smc = (Axis2MessageContext) messageContext;
            org.apache.axis2.context.MessageContext axis2MessageCtx = axis2smc.getAxis2MessageContext();
            axis2MessageCtx.getOperationContext().setProperty(RESPONSE_WRITTEN, "SKIP");
            return false;
        }
        return true;
    }

    @Override
    public boolean handleResponseOutFlow(MessageContext messageContext) {
        return true;
    }

    private void handleException(String msg, Exception e, MessageContext msgContext) {
        log.error(msg, e);
        if (msgContext.getServiceLog() != null) {
            msgContext.getServiceLog().error(msg, e);
        }
        throw new SynapseException(msg, e);
    }
}
