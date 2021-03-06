# 
# Copyright 2010 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Tagfiler client.
"""

from tagfiler.iobox.models import File

from httplib import HTTPConnection, HTTPSConnection
from httplib import OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER
import urlparse
import urllib
import logging
import socket

try:
    import simplejson #@UnresolvedImport
    json = simplejson
except:
    import json as imported_json
    json = imported_json


logger = logging.getLogger(__name__)


class TagfilerException(Exception):
    def __init__(self, value, cause=None):
        super(TagfilerException, self).__init__(value)
        self.value = value
        self.cause = cause
        
    def __str__(self):
        message = "%s." % self.value
        if self.cause:
            message += " Caused by: %s." % self.cause
        return message

class MalformedURL(TagfilerException):
    """MalformedURL indicates a malformed URL.
    """
    def __init__(self, cause=None):
        super(MalformedURL, self).__init__("URL was malformed", cause)

class UnresolvedAddress(TagfilerException):
    """UnresolvedAddress indicates a failure to resolve the network address of
    the Tagfiler service.
    
    This error is raised when a low-level socket.gaierror is caught.
    """
    def __init__(self, cause=None):
        super(UnresolvedAddress, self).__init__("Could not resolve address of host", cause)

class NetworkError(TagfilerException):
    """NetworkError wraps a socket.error exception.
    
    This error is raised when a low-level socket.error is caught.
    """
    def __init__(self, cause=None):
        super(NetworkError, self).__init__("Network I/O failure", cause)

class ProtocolError(TagfilerException):
    """ProtocolError indicates a protocol-level failure.
    
    In other words, you may have tried to add a tag for which no tagdef exists.
    """
    def __init__(self, message='Network protocol failure', errorno=-1, response=None, cause=None):
        super(ProtocolError, self).__init__("Tagfiler protocol failure", cause)
        self._errorno = errorno
        self._response = response
        
    def __str__(self):
        message = "%s." % self.value
        if self._errorno >= 0:
            message += " HTTP ERROR %d: %s" % (self._errorno, self._response)
        return message
    
class NotFoundError(TagfilerException):
    """Raised for HTTP NOT_FOUND (i.e., ERROR 404) responses."""
    pass


class TagfilerClient(object):
    """Web service client used to interact with the Tagfiler REST service."""

    def __init__(self, url, username=None, password=None, goauthtoken=None):
        """Initializes the Tagfiler client object.
        """
        pieces = urlparse.urlparse(url) #TODO: does this throw exceptions?!
        
        self.scheme = pieces[0]
        host_port = pieces[1].split(":")
        self.host = host_port[0]
        self.port = None
        if len(host_port) > 1:
            self.port = host_port[1]
        self.baseuri = pieces[2]
        self.username = username
        self.password = password
        self.goauthtoken = goauthtoken
        self.connection_class = None
        if self.scheme == "https":
            self.connection_class = HTTPSConnection
        else:
            self.connection_class = HTTPConnection
        self.connection = None
        
        self.authn_header = None
        self.authn_header_value = None
        #self.cookie = None
        
        if not self.host or not len(self.host):
            raise MalformedURL(cause='Hostname cannot be None')
        
        if self.port and len(self.port):
            try:
                self.port = int(self.port)
            except Exception:
                raise MalformedURL(cause='Invalid port number (%s)' % self.port)


    def connect(self):
        """Connects to the Tagfiler service."""
        assert not self.connection
        self.connection = self.connection_class(host=self.host, port=self.port)


    def login(self):
        """Login to the Tagfiler service.
        
        Raises 'UnresolvedAddress' if unable to resolve the hostname.
        
        Raises 'NotFoundError' if the Tagfiler login resource is not found.
        """
        assert self.connection
        assert self.goauthtoken or (self.username and self.password)
        if self.goauthtoken:
            # This is not the Login you are looking for
            self.authn_header = 'Globus-Goauthtoken'
            self.authn_header_value = self.goauthtoken
        else:
            # Session authentication
            headers = {}
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            try:
                loginurl = "%s/session" % self.baseuri
                resp = self._send_request("POST", loginurl, 
                                          "username=%s&password=%s" % \
                                          (self.username, self.password), headers)
                #self.cookie = resp.getheader("set-cookie")
                self.authn_header_value = resp.getheader("set-cookie")
                self.authn_header = 'Cookie'
            except socket.gaierror as e:
                raise UnresolvedAddress(e)
            except ProtocolError as e:
                raise


    def close(self):
        """Closes the connection to the Tagfiler service.
        
        The underlying python documentation is not very helpful but it would
        appear that the HTTP[S]Connection.close() could raise a socket.error.
        Thus, this method potentially raises a 'NetworkError'.
        """
        assert self.connection
        try:
            self.connection.close()
        except socket.error as e:
            raise NetworkError(e)
        finally:
            self.connection = None
            self.authn_header = None
            self.authn_header_value = None
            #self.cookie = None


    def _send_request(self, method, url, body='', headers={}):
        try:
            self.connection.request(method, url, body, headers)
            resp = self.connection.getresponse()
            if resp.status not in [OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER]:
                raise ProtocolError(errorno=resp.status, response=resp.read())
        except socket.error as e:
            raise NetworkError(e)
        return resp


    def add_subjects(self, fileobjs):
        """Registers a list of files and tags in tagfiler using a single request.
        
        Keyword arguments:
        
        fileobjs -- the list of register files objects 
        
        """
        parsed_table = []
        tag_names = []
        
        tag_sets = []
        for fileobj in fileobjs:
            tag_sets.append(fileobj.tags)
            tag_sets.extend(fileobj.content_tags)
        
        for tag_set in tag_sets:
            # TODO: need to remove the following comment. 'name' used to be 
            #   required so we would catch it here and raise an error, but
            #   that is no longer the case.
            #
            # name is a required tag
            #if not len(fileobj.filter_tags("name")):
            #    raise ValueError("Register file %s must have its 'name' tag set." % unicode(fileobj))
            parsed_dict = {}
            for tag in tag_set:
                tag_list = parsed_dict.get(tag.name, [])
                tag_list.append(tag.value)
                parsed_dict[tag.name] = tag_list
                if tag.name != "name":
                    if tag.name not in tag_names:
                        tag_names.append(tag.name)
            parsed_table.append(parsed_dict)
        payload = json.dumps(parsed_table)
        bulkurl = '%s/subject/name(%s)' % (self.baseuri, ';'.join([ self._safequote(tag) for tag in tag_names ]))
        #headers = {"Content-Type": "application/json", "Cookie": self.cookie}
        headers = {"Content-Type": "application/json", \
                   self.authn_header: self.authn_header_value}
        self._send_request("PUT", bulkurl, payload, headers)


    def find_subject_by_name(self, name):
        """Looks up a subject by its name tag in tagfiler and returns a dictionary if found, None otherwise
        
        Keyword arguments:
        name -- name to query
        """
        url = "%s/tags/name=%s" % (self.baseuri, self._safequote(name))
        # TODO (RS): need to remove "Cookie" references
        #headers = {"Cookie": self.cookie, "Accept": "application/json"}
        headers = {"Accept": "application/json", \
                   self.authn_header: self.authn_header_value}
        resp = self._send_request("GET", url, headers=headers)
        subject = json.loads(resp.read())
        return subject


    def _safequote(self, s):
        return urllib.quote(s, '')


    '''
    TODO: this commented code should be removed. I'm only keep it temporarily, 
    because I will not remember how to do this non-bulk call!
    
    def add_subject(self, fileobj):
        """Registers a single file in tagfiler
        
        Keyword arguments:
        fileobj -- models.File object with tags
        """
        assert isinstance(fileobj, File)

        # name is a required tag
        if not len(fileobj.filter_tags("name")):
            raise ValueError("Register file %s must have its 'name' tag set." % unicode(fileobj))

        # Remove the name tag from the file tags, since this is specified outside the query string
        tag_pairs = []
        for tag in fileobj.tags:
            if tag.name != "name":
                tag_pairs.append("%s=%s" % (self._safequote(tag.name), self._safequote(tag.value)))
        url = "%s/subject/name=%s?%s" % (self.baseuri, 
                    self._safequote(fileobj.filter_tags("name")[0].value), "&".join(tag_pairs))
        headers = {"Cookie": self.cookie}
        self._send_request("PUT", url, headers=headers)

    '''