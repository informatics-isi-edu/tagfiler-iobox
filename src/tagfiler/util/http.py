'''
Created on Sep 14, 2012

@author: smithd
'''
import urlparse
import urllib
import logging
from httplib import HTTPConnection, HTTPSConnection, OK, CREATED, ACCEPTED, NO_CONTENT, HTTPException, SEE_OTHER

logger = logging.getLogger(__name__)

try:
    import simplejson
    
    jsonWriter = simplejson.dumps
    jsonReader = simplejson.loads
    jsonFileReader = simplejson.load
except:
    import json

    if hasattr(json, 'dumps'):
        jsonWriter = json.dumps
        jsonReader = json.loads
        jsonFileReader = json.load
    else:
        raise RuntimeError('Could not configure JSON library.')

class TagfilerClient(object):
    """Web service client used to interact with the Tagfiler REST service.
    
    """
    def __init__(self, config, **kwargs):
        """Constructor
        
        Keyword arguments:
        config -- tagfiler configuration object
        
        """
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

    def _create_connection(self):
        return self.connection_class(host=self.host, port=self.port)
    
    def _login(self, connection):
        headers = {}
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        resp = self._send_request(connection, "POST", "/webauthn/login", "username=%s&password=%s" % (self.username, self.password), headers)
        return resp.getheader("set-cookie")
    
    def _send_request(self, connection, method, url, body='', headers={}):
        connection.request(method, url, body, headers)
        resp = connection.getresponse()
        if resp.status not in [OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER]:
            raise HTTPException("Error response (%i) received: %s" % (resp.status, resp.read()))
        return resp
    
    def add_subjects(self, register_files):
        """Registers a list of files and tags in tagfiler using a single request.
        
        Keyword arguments:
        
        register_files -- the list of register files objects 
        
        """
        parsed_table = []
        tag_names = []
        for register_file in register_files:
            parsed_dict = {}
            for tag in register_file.get_tags():
                tag_list = parsed_dict.get(tag.get_tag_name(), [])
                tag_list.append(tag.get_tag_value())
                parsed_dict[tag.get_tag_name()] = tag_list
                if tag.get_tag_name() != "name":
                    if tag.get_tag_name() not in tag_names:
                        tag_names.append(tag.get_tag_name())
            parsed_table.append(parsed_dict)
        payload = jsonWriter(parsed_table)
        bulkurl = '%s/subject/name(%s)' % (self.baseuri, ';'.join([ self._safequote(tag) for tag in tag_names ]))
        connection = self._create_connection()
        login_cookie = self._login(connection)
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Cookie"] = login_cookie
        self._send_request(connection, "PUT", bulkurl, payload, headers)
        connection.close()

    def add_subject(self, register_file):
        """Registers a single file in tagfiler
        
        Keyword arguments:
        register_file -- register file object with tags
        """
        # Remove the name tag from the file tags, since this is specified outside the query string
        tag_pairs = []
        for tag in register_file.get_tags():
            if tag.get_tag_name() != "name":
                tag_pairs.append("%s=%s" % (self._safequote(tag.get_tag_name()), self._safequote(tag.get_tag_value())))
        url = "%s/subject/name=%s?%s" % (self.baseuri, self._safequote(register_file.get_tag("name")[0].get_tag_value()), "&".join(tag_pairs))
        connection = self._create_connection()
        login_cookie = self._login(connection)
        headers = {}
        headers["Cookie"] = login_cookie
        self._send_request(connection, "PUT", url, headers=headers)
        connection.close()

    def find_subject_by_name(self, name):
        """Looks up a subject by its name tag in tagfiler and returns a dictionary if found, None otherwise
        
        Keyword arguments:
        name -- name to query
        """
        subject = None
        url = "%s/tags/name=%s" % (self.baseuri, self._safequote(name))
        
        connection = self._create_connection()
        login_cookie = self._login(connection)
        headers = {}
        headers["Cookie"] = login_cookie
        headers["Accept"] = "application/json"
        try:
            resp = self._send_request(connection, "GET", url, headers=headers)
            subject = jsonReader(resp.read())
        except HTTPException,e:
            logger.error(e)
        connection.close()
        return subject

    def _safequote(self, s):
        return urllib.quote(s, '')
