# -*- coding: UTF-8 -*-
# Copyright 2013 p0is0n (poisonoff@gmail.com).
#
# This file is part of Mail-Services.
#
# Mail-Services is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Mail-Services is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Mail-Services.  If not, see <http://www.gnu.org/licenses/>.

import os

from time import time
from types import ListType, TupleType
from functools import partial

from twisted.python.log import msg, err
from twisted.internet import reactor
from twisted.internet import error
from twisted.internet import ssl

from twisted.internet.defer import Deferred, DeferredLock, DeferredList, DeferredSemaphore
from twisted.internet.defer import DeferredQueue, inlineCallbacks, returnValue
from twisted.internet.defer import fail, succeed
from twisted.internet.reactor import callLater

from twisted.mail.smtp import ESMTPSenderFactory as _ESMTPSenderFactory, ESMTPSender as _ESMTPSender, SMTPConnectError, SMTPProtocolError
from twisted.mail.smtp import ESMTPClient as _ESMTPClient, DNSNAME, SUCCESS, quoteaddr, SMTPProtocolError, SMTPDeliveryError, SMTPClientError
from twisted.mail.smtp import CramMD5ClientAuthenticator, LOGINAuthenticator, PLAINAuthenticator
from twisted.mail import relaymanager
from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory, Protocol
from twisted.protocols.basic import FileSender

from core.constants import DEBUG


class ESMTPSender(_ESMTPSender):

    debug = DEBUG

    def lineReceived(self, line):
        if DEBUG:
            msg('ESMTPSender', 'SMTP <<<', repr(line))

        # Success
        return _ESMTPSender.lineReceived(self, line)

    def sendLine(self, line):
        if DEBUG:
            msg('ESMTPSender', 'SMTP >>>', repr(line))

        # Success
        return _ESMTPSender.sendLine(self, line)


class ESMTPSenderFactory(_ESMTPSenderFactory):

    noisy = DEBUG
    protocol = ESMTPSender

    def _processConnectionError(self, connector, err):
        if self.retries < self.sendFinished <= 0:
            # Rewind the file in case part of it was read while attempting to send the message.
            self.file.seek(0, 0)

            # Try
            connector.connect()
            self.retries += 1
        elif self.sendFinished <= 0:
            if err.check(error.ConnectionDone):
                err.value = SMTPConnectError(-1, "Unable to connect to server.")

            self.result.errback(err.value)


class ESMTPClient(_ESMTPClient):

    heloFallback = True

    requireTransportSecurity = False
    requireAuthentication = False

    connected = False
    debug = DEBUG

    def __init__(self, username, secret, contextFactory=None, *args, **kwargs):
        self._requestDeferred = None
        self._requestParams = None

        self._lastreq = None

        if username:
            self.heloFallback = 0
            self.username = username
        else:
            self.username = ''

        if contextFactory is None:
            contextFactory = self._getContextFactory()

        (_ESMTPClient.__init__(
            self,
            secret,
            contextFactory,
            *args,
            **kwargs
        ))

        if username:
            self._registerAuthenticators()

    def lineReceived(self, line):
        if DEBUG:
            msg('ESMTPClient', 'SMTP <<<', repr(line), self.factory)

        # Success
        return _ESMTPClient.lineReceived(self, line)

    def sendLine(self, line):
        if DEBUG:
            msg('ESMTPClient', 'SMTP >>>', repr(line), self.factory)

        # Success
        return _ESMTPClient.sendLine(self, line)

    def _registerAuthenticators(self):
        self.registerAuthenticator(CramMD5ClientAuthenticator(self.username))
        self.registerAuthenticator(LOGINAuthenticator(self.username))
        self.registerAuthenticator(PLAINAuthenticator(self.username))

    def _getContextFactory(self):
        if self.context is not None:
            return self.context

        try:
            context = ssl.ClientContextFactory()
            context.method = ssl.SSL.TLSv1_METHOD

            return context
        except AttributeError:
            return None

    def smtpState_from(self, code, resp):
        self.sendLine('NOOP')

        self._expected = SUCCESS
        self._okresponse = self.pool_done
        self._failresponse = self.smtpTransferFailed

    def smtpState_noop(self, code, resp):
        """Ignore this command"""

    def sendError(self, exception):
        """
        If an error occurs before a mail message is sent sendError will be
        called.  This base class method sends a QUIT if the error is
        non-fatal and disconnects the connection.

        @param exc: The SMTPClientError (or child class) raised
        @type exc: C{SMTPClientError}
        """

        self.factory.protocolErrors += 1
        self.factory.protocolError = exception

        if isinstance(exception, SMTPClientError) and not exception.isFatal:
            err(exception)

            # Close connection
            self._disconnectFromServer()
        else:
            # If the error was fatal then the communication channel with the
            # SMTP Server is broken so just close the transport connection
            self.smtpState_disconnect(-1, None)

    def connectionMade(self):
        if DEBUG:
            msg('ESMTPClient', 'connection made', self, self.factory)

        return _ESMTPClient.connectionMade(self)

    def connectionLost(self, reason):
        if DEBUG:
            msg('ESMTPClient', 'connection lost', self, self.factory)

        self.connected = False

        if self._requestDeferred is not None:
            callLater(0.1, self._requestDeferred.errback, reason)

            # Fail
            self._requestDeferred = None
            self._requestParams = None

        return _ESMTPClient.connectionLost(self, reason)

    def pool_done(self, *args):
        """Pool done client"""

        if DEBUG:
            msg('ESMTPClient', 'done', self, self.factory)

        self._lastreq = time()

        self.connected = True
        self.factory.resetDelay()

        if self.factory.deferred is not None:
            self.factory.deferred.callback(self)

            # Clean deferred
            self.factory.deferred = None
        else:
            # pool pendings
            self.factory.pool.delPendings(self.factory)

            # Will is reconnect action
            self.factory.pool.clientFree(self)

    def pool_free(self, *args):
        """Pool free client"""

        if DEBUG:
            msg('ESMTPClient', 'free', self, self.factory)

        self._lastreq = time()

    def pool_lost(self, *args):
        """Pool lost client"""

        if DEBUG:
            msg('ESMTPClient', 'lost', self, self.factory)

    def sendMail(self, **params):
        assert (self._requestDeferred is None and self._requestParams is None), ('Wow! Already set request "{0}" "{1}"'.format(
            repr(self._requestDeferred),
            repr(self._requestParams),
        ))

        # Set current request
        self._requestDeferred = Deferred()
        self._requestParams = params

        if not isinstance(self._requestParams['to'], (ListType, TupleType)):
            self._requestParams['to'] = tuple((params['to'], ))
            self._requestParams['toIter'] = iter(params['to'])
        else:
            self._requestParams['to'] = tuple(params['to'])
            self._requestParams['toIter'] = iter(params['to'])

        self.sendLine('RSET')

        self._expected = SUCCESS
        self._okresponse = self._sendMail_from
        self._failresponse = self._sendMail_fail

        # Success
        return self._requestDeferred

    def _sendMail_sent(self, code, resp):
        if self._requestDeferred is not None:
            callLater(0.1, self._requestDeferred.callback, (code, self._requestParams['to']))

            # Fail
            self._requestDeferred = None
            self._requestParams = None

        self._expected = SUCCESS
        self._okresponse = self.smtpState_noop
        self._failresponse = self._sendMail_fail

    def _sendMail_fail(self, code, resp):
        if self._requestDeferred is not None:
            callLater(0.1, self._requestDeferred.errback, SMTPDeliveryError(
                code,
                resp, self.log.str()
            ))

            # Fail
            self._requestDeferred = None
            self._requestParams = None
        else:
            # Simple error, disconnect
            self.sendError(SMTPProtocolError(code, resp, self.log.str()))

    def _sendMail_from(self, code, resp):
        self.sendLine('MAIL FROM:%s' % quoteaddr(self._requestParams['sender']))

        self._expected = SUCCESS
        self._okresponse = self._sendMail_to
        self._failresponse = self._sendMail_fail

    def _sendMail_to(self, code, resp):
        try:
            self.sendLine('RCPT TO:%s' % quoteaddr(self._requestParams['toIter'].next()))
        except StopIteration:
            self.sendLine('DATA')

            self._expected = [354]
            self._okresponse = self._sendMail_data
            self._failresponse = self._sendMail_fail
        else:
            self._expected = SUCCESS
            self._okresponse = self._sendMail_to
            self._failresponse = self._sendMail_fail

    def _sendMail_data(self, code, resp):
        transfer = FileSender()

        deferred = transfer.beginFileTransfer(self._requestParams['file'], self.transport, self.transformChunk)
        deferred.addCallbacks(self.finishedFileTransfer, self.sendError)

        self._expected = SUCCESS
        self._okresponse = self._sendMail_sent
        self._failresponse = self._sendMail_fail


class ESMTPFactory(ReconnectingClientFactory):

    noisy = DEBUG
    domain = DNSNAME

    maxDelay = 5
    maxRetries = 5

    protocol = ESMTPClient
    protocolInstance = None

    protocolError = None
    protocolErrors = 0

    def __init__(self, username, password, contextFactory=None, requireAuthentication=False):
        self.deferred = Deferred()

        self.username = username
        self.password = password
        self.contextFactory = contextFactory
        self.requireAuthentication = requireAuthentication

        self.errors = 0
        self.errorsLimit = 5

    def startedConnecting(self, connector):
        self.connector = connector

    def clientConnectionLost(self, connector, reason):
        """
        Notify the pool that we've lost our connection.
        """
        self.errors += 1

        # Stop
        if self.errors >= self.errorsLimit:
            self.stopTrying()

        if self.continueTrying:
            if self.protocolInstance is not None:
                if self.protocolInstance._lastreq and ((time() - self.protocolInstance._lastreq) >= 100):
                    self.stopTrying()

        if self.continueTrying:
            ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

            # pool pendings
            self.pool.addPendings(self)
        else:
            if self.deferred:
                if self.protocolError is not None:
                    self.deferred.errback(self.protocolError)
                else:
                    self.deferred.errback(reason)

                # Clean deferred
                self.deferred = None
            else:
                # pool pendings
                self.pool.delPendings(self)

        if self.protocolInstance is not None:
            self.pool.clientGone(self.protocolInstance)

        # Clean
        self.protocolInstance = None
        self.protocolError = None

    def clientConnectionFailed(self, connector, reason):
        """
        Notify the pool that we're unable to connect
        """
        self.errors += 1

        # Stop
        if self.errors >= self.errorsLimit:
            self.stopTrying()

        if self.continueTrying:
            if self.protocolInstance is not None:
                if self.protocolInstance._lastreq and ((time() - self.protocolInstance._lastreq) >= 100):
                    self.stopTrying()

        if self.continueTrying:
            ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

            # pool pendings
            self.pool.addPendings(self)
        else:
            if self.deferred:
                if self.protocolError is not None:
                    self.deferred.errback(self.protocolError)
                else:
                    self.deferred.errback(reason)

                # Clean deferred
                self.deferred = None
            else:
                # pool pendings
                self.pool.delPendings(self)

        if self.protocolInstance is not None:
            self.pool.clientGone(self.protocolInstance)

        # Clean
        self.protocolInstance = None
        self.protocolError = None

    def buildProtocol(self, addr):
        """
        Attach the C{self.pool} to the protocol so it can tell it, when we've connected.
        """
        if self.protocolInstance is not None:
            self.pool.clientGone(self.protocolInstance)

        self.protocolInstance = self.protocol(self.username, self.password, self.contextFactory, self.domain, 1)
        self.protocolInstance.requireAuthentication = self.requireAuthentication
        self.protocolInstance.factory = self

        self.resetDelay()

        return self.protocolInstance

    def __str__(self):
        return '<ESMTPFactory %02d instance at 0x%X>' % (
            self.counter, id(self))

    __repr__ = __str__


class ESMTPSenderPoolError(Exception):
    """Poll error"""


class ESMTPSenderPool(object):
    """Pool of smtp connections"""

    def __init__(self, poolsize=1, **params):
        self._busyClients = set([])
        self._freeClients = set([])

        self._pendingsList = set([])

        self._commands = []
        self._poolsize = poolsize
        self._partials = dict()

        self._params = params
        self._counter = 0

        self.errors = 0
        self.errorsReconnecting = 0
        self.errorsReconnectingLimit = 5

    def closeConnections(self):
        if self._commands:
            # Cancel
            for deferred, method, args, kwargs in self._commands:
                deferred.errback(ESMTPSenderPoolError(
                    'Close connection'))

            # Clean
            self._commands[:] = []

        f = self._pendingsList.copy()
        c = self._freeClients.union(self._busyClients).copy()

        self._pendingsList.clear()

        self._busyClients.clear()
        self._freeClients.clear()

        for factory in f:
            factory.stopTrying()
            factory.continueTrying = 0

            if factory.connector.state in ('connecting', 'connected'):
                try:
                    factory.connector.stopConnecting()
                    factory.connector.disconnect()
                except:
                    # Skip error
                    pass

        for client in c:
            client.factory.stopTrying()
            client.factory.continueTrying = 0

            if client.factory.connector.state in ('connecting', 'connected'):
                try:
                    client.factory.connector.stopConnecting()
                    client.factory.connector.disconnect()
                except:
                    # Skip error
                    pass

            client.transport.loseConnection()

    def addPendings(self, factory):
        if DEBUG:
            msg('ESMTPSenderPool', 'addPendings', factory)

        if not factory in self._pendingsList:
            # Add factory to pendings list
            self._pendingsList.add(factory)

    def delPendings(self, factory):
        if DEBUG:
            msg('ESMTPSenderPool', 'delPendings', factory)

        if factory in self._pendingsList:
            # Delete factory from pendings list
            self._pendingsList.remove(factory)

    def clientConnect(self):
        if DEBUG:
            msg('ESMTPSenderPool', 'clientConnect')

        self._counter += 1

        factory = (ESMTPFactory(
            self._params['username'],
            self._params['password'],
            requireAuthentication=bool(self._params['username'] and self._params['password'])
        ))
        factory.pool = self
        factory.counter = self._counter

        self.addPendings(factory)

        if bool(self._params.get('ssl')):
            (reactor.connectSSL(
                self._params['hostname'],
                self._params['portname'],
                factory,
                contextFactory,
                timeout=10,
            ))
        else:
            (reactor.connectTCP(
                self._params['hostname'],
                self._params['portname'],
                factory,
                timeout=10,
            ))

        def _cb(client):
            self.errorsReconnecting = 0
            self.delPendings(factory)

            if client is not None:
                self.clientFree(client)

        def _eb(reason):
            if reason:
                msg('ESMTPSenderPool', self._params['hostname'], self._params['portname'])
                err(reason)

            self.errorsReconnecting += 1
            self.delPendings(factory)

            # Retry
            if self._commands:
                if self.errorsReconnecting <= self.errorsReconnectingLimit:
                    if not (len(self._busyClients) + len(self._pendingsList)) >= self._poolsize:
                        self.clientConnect()
                else:
                    if not self._pendingsList:
                        # Cancel
                        for deferred, method, args, kwargs in self._commands:
                            deferred.errback(ESMTPSenderPoolError(
                                'Cannot connect to any server, please check you connection params to smtp'))

                        # Clean
                        self._commands[:] = []

        deferred = factory.deferred
        deferred.addCallbacks(_cb, _eb)

        # Wait for new connection
        return deferred

    def pendingCommand(self, method, args, kwargs):
        if DEBUG:
            msg('ESMTPSenderPool', 'pendingCommand', method, args, kwargs)

        deferred = Deferred()

        if deferred:
            self._commands.append((deferred, method, args, kwargs))

        # Wait in this deferred
        return deferred

    def performRequestOnClient(self, client, method, args, kwargs):
        if DEBUG:
            msg('ESMTPSenderPool', 'performRequestOnClient', client, client.factory, method)

        self.clientBusy(client)

        # Request
        requestOnClient = getattr(client, method, None)
        if requestOnClient is not None:
            def _cb(result, client=client):
                self.clientFree(client)

                # Got result on next callback
                return result

            return requestOnClient(*args, **kwargs).addBoth(_cb)

        # Fail, method not exists
        self.clientFree(client)

        return fail(AttributeError(client, method))

    def performRequest(self, method, *args, **kwargs):
        if DEBUG:
            msg('ESMTPSenderPool', 'performRequest', method, args, kwargs)

        if len(self._freeClients):
            return self.performRequestOnClient(
                self._freeClients.pop(), method, args, kwargs)

        if not (len(self._busyClients) + len(self._pendingsList)) >= self._poolsize:
            self.clientConnect()

        # Wait for request
        return self.pendingCommand(method, args, kwargs)

    def clientGone(self, client):
        """
        Notify that the given client is to be removed from the pool completely.

        @param client: An instance of a L{Protocol}.
        """
        if DEBUG:
            msg('ESMTPSenderPool', 'clientGone', client, client.factory)

        if client in self._busyClients:
            self._busyClients.remove(client)

        if client in self._freeClients:
            self._freeClients.remove(client)

    def clientBusy(self, client):
        """
        Notify that the given client is being used to complete a request.

        @param client: An instance of a L{Protocol}.
        """
        if DEBUG:
            msg('ESMTPSenderPool', 'clientBusy', client, client.factory)

        if client in self._freeClients:
            self._freeClients.remove(client)

        self._busyClients.add(client)

    def clientFree(self, client):
        """
        Notify that the given client is free to handle more requests.

        @param client: An instance of a L{Protocol}.
        """
        if DEBUG:
            msg('ESMTPSenderPool', 'clientFree', client, client.factory, client.connected)

        if client in self._busyClients:
            self._busyClients.remove(client)

        # Free client
        if client.connected:
            self._freeClients.add(client)

            if len(self._commands):
                (deferred, method, args, kwargs) = self._commands.pop(0)

                # With chain deferred
                self.performRequestOnClient(self._freeClients.pop(), method,
                    args, kwargs).chainDeferred(deferred)

    def __getattr__(self, name):
        try:
            # Return method if exists
            return self._partials[name]
        except KeyError:
            self._partials[name] = (partial(
                self.performRequest, name
            ))

            # Success
            return self._partials[name]
