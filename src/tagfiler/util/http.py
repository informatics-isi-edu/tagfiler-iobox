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

from tagfiler.iobox.models import Tagfiler, File

import urlparse
import urllib
import logging
from httplib import HTTPConnection, HTTPSConnection, HTTPException
from httplib import OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER

try:
    import simplejson #@UnresolvedImport
    json = simplejson
except:
    import json as imported_json
    json = imported_json


logger = logging.getLogger(__name__)


class TagfilerClient(object):
    """Web service client used to interact with the Tagfiler REST service.
    """
    def __init__(self, config, **kwargs):
        """Initializes the object.
        
        The 'config' parameter is expected to be a Tagfiler model object.
        """
        assert isinstance(config, Tagfiler)

        pieces = urlparse.urlparse(config.get_url())
        
        self.scheme = pieces[0]
        host_port = pieces[1].split(":")
        self.host = host_port[0]
        self.port = None
        if len(host_port) > 1:
            self.port = host_port[1]
        self.baseuri = pieces[2]
        self.username = config.get_username()
        self.password = config.get_password()
        self.connection_class = None
        if self.scheme == "https":
            self.connection_class = HTTPSConnection
        else:
            self.connection_class = HTTPConnection
        self.connection = None
        self.cookie = None

    def connect(self):
        """Connects to the Tagfiler service."""
        #TODO: catch/raise http exceptions
        #TODO: throw exception if connection is not None!
        self.connection = self.connection_class(host=self.host, port=self.port)
    
    def login(self):
        """Login to the Tagfiler service."""
        #TODO: catch/raise http exceptions
        #TODO: throw exception if connection is None!
        headers = {}
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        resp = self._send_request("POST", "/webauthn/login", 
                                  "username=%s&password=%s" % \
                                  (self.username, self.password), headers)
        self.cookie = resp.getheader("set-cookie")
    
    def close(self):
        """Closes the connection to the Tagfiler service."""
        #TODO: catch/raise http exceptions
        #TODO: throw exception if connection is None!
        self.connection.close()
        self.connection = None
        self.cookie = None
    
    def _send_request(self, method, url, body='', headers={}):
        self.connection.request(method, url, body, headers)
        resp = self.connection.getresponse()
        if resp.status not in [OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER]:
            raise HTTPException("Error response (%i) received: %s" % (resp.status, resp.read()))
        return resp
    
    def add_subjects(self, fileobjs):
        """Registers a list of files and tags in tagfiler using a single request.
        
        Keyword arguments:
        
        fileobjs -- the list of register files objects 
        
        """
        parsed_table = []
        tag_names = []
        for fileobj in fileobjs:
            # name is a required tag
            if fileobj.get_tag("name") is None or len(fileobj.get_tag("name")) == 0:
                raise ValueError("Register file %s must have its 'name' tag set." % unicode(fileobj))
            parsed_dict = {}
            for tag in fileobj.get_tags():
                tag_list = parsed_dict.get(tag.get_tag_name(), [])
                tag_list.append(tag.get_tag_value())
                parsed_dict[tag.get_tag_name()] = tag_list
                if tag.get_tag_name() != "name":
                    if tag.get_tag_name() not in tag_names:
                        tag_names.append(tag.get_tag_name())
            parsed_table.append(parsed_dict)
        payload = json.dumps(parsed_table)
        bulkurl = '%s/subject/name(%s)' % (self.baseuri, ';'.join([ self._safequote(tag) for tag in tag_names ]))
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Cookie"] = self.cookie
        self._send_request("PUT", bulkurl, payload, headers)

    def add_subject(self, fileobj):
        """Registers a single file in tagfiler
        
        Keyword arguments:
        fileobj -- models.File object with tags
        """
        assert isinstance(fileobj, File)

        # name is a required tag
        if fileobj.get_tag("name") is None or len(fileobj.get_tag("name")) == 0:
            raise ValueError("Register file %s must have its 'name' tag set." % unicode(fileobj))

        # Remove the name tag from the file tags, since this is specified outside the query string
        tag_pairs = []
        for tag in fileobj.get_tags():
            if tag.get_tag_name() != "name":
                tag_pairs.append("%s=%s" % (self._safequote(tag.get_tag_name()), self._safequote(tag.get_tag_value())))
        url = "%s/subject/name=%s?%s" % (self.baseuri, self._safequote(fileobj.get_tag("name")[0].get_tag_value()), "&".join(tag_pairs))
        headers = {}
        headers["Cookie"] = self.cookie
        self._send_request("PUT", url, headers=headers)

    def find_subject_by_name(self, name):
        """Looks up a subject by its name tag in tagfiler and returns a dictionary if found, None otherwise
        
        Keyword arguments:
        name -- name to query
        """
        subject = None
        url = "%s/tags/name=%s" % (self.baseuri, self._safequote(name))
        
        headers = {}
        headers["Cookie"] = self.cookie
        headers["Accept"] = "application/json"
        try:
            resp = self._send_request("GET", url, headers=headers)
            subject = json.loads(resp.read())
        except HTTPException,e:
            logger.error(e)
        return subject

    def _safequote(self, s):
        return urllib.quote(s, '')
