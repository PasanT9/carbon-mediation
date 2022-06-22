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

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.engine.AxisEngine;
import org.apache.axis2.engine.Handler;
import org.apache.axis2.transport.TransportSender;
import org.apache.axis2.util.LoggingControl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;

/**
 * This class is used to invoke a message from the server to the client with streaming capabilities.
 *
 */
public class ODataAxisEngine extends AxisEngine {
    private static final Log log = LogFactory.getLog(AxisEngine.class);
    private static boolean RESUMING_EXECUTION = true;

    private static Handler.InvocationResponse invoke(MessageContext msgContext, boolean resuming) throws AxisFault {
        if (msgContext.getCurrentHandlerIndex() == -1) {
            msgContext.setCurrentHandlerIndex(0);
        }
        Handler.InvocationResponse pi = Handler.InvocationResponse.CONTINUE;
        while (msgContext.getCurrentHandlerIndex() < msgContext.getExecutionChain().size()) {
            Handler currentHandler = (Handler) msgContext.getExecutionChain().get(msgContext.getCurrentHandlerIndex());
            try {
                if (!resuming) {
                    msgContext.addExecutedPhase(currentHandler);
                } else {
                    resuming = false;
                }
                pi = currentHandler.invoke(msgContext);
            } catch (AxisFault var5) {
                if (msgContext.getCurrentPhaseIndex() == 0) {
                    msgContext.removeFirstExecutedPhase();
                }
                throw var5;
            }
            if (pi.equals(Handler.InvocationResponse.SUSPEND) || pi.equals(Handler.InvocationResponse.ABORT)) {
                break;
            }
            msgContext.setCurrentHandlerIndex(msgContext.getCurrentHandlerIndex() + 1);
        }
        return pi;
    }

    private static void flowComplete(MessageContext msgContext) {
        Iterator<Handler> invokedPhaseIterator = msgContext.getExecutedPhases();
        while (invokedPhaseIterator.hasNext()) {
            Handler currentHandler = (Handler) invokedPhaseIterator.next();
            currentHandler.flowComplete(msgContext);
        }
        msgContext.resetExecutedPhases();
    }

    /**
     * This method invokes a streaming response from the server to the client.
     *
     * @param messageContext
     * @param response
     * @return
     * @throws AxisFault
     */
    public static Handler.InvocationResponse stream(MessageContext messageContext, ODataServletResponse response)
            throws AxisFault {
        if (LoggingControl.debugLoggingAllowed && log.isTraceEnabled()) {
            log.trace(messageContext.getLogIDString() + " stream:" + messageContext.getMessageID());
        }
        Handler.InvocationResponse pi = invoke(messageContext, RESUMING_EXECUTION);
        if (pi.equals(Handler.InvocationResponse.CONTINUE)) {
            TransportOutDescription transportOut = messageContext.getTransportOut();
            TransportSender sender = transportOut.getSender();
            ODataTransportSender ODataTransportSender = new ODataTransportSender(sender,
                                                                                 messageContext.getConfigurationContext(),
                                                                                 transportOut, response);
            ODataTransportSender.invoke(messageContext);
            flowComplete(messageContext);
        }
        return pi;
    }
}
