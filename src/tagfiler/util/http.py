'''
Created on Sep 14, 2012

@author: smithd
'''
import urlparse
import urllib
from httplib import HTTPConnection, HTTPSConnection, OK, CREATED, ACCEPTED, NO_CONTENT, HTTPException

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
        resp = self.send_request(connection, "POST", "/webauthn/login", "username=%s&password=%s" % (self.username, self.password), headers)
        return resp.getheader("set-cookie")
    
    def _send_request(self, connection, method, url, body='', headers={}):
        connection.request(method, url, body, headers)
        resp = connection.getresponse()
        if resp.status not in [OK, CREATED, ACCEPTED, NO_CONTENT]:
            raise HTTPException("Error response (%i) received: %s" % (resp.status, resp.read()))
        return resp
    
    def register_files(self, register_files):
        """Registers a list of files and tags in tagfiler using a single request.
        
        Keyword arguments:
        
        register_files -- the list of register files objects 
        
        """
        parsed_table = []
        tag_names = set()
        for register_file in register_files:
            parsed_dict = {}
            for tag in register_file.get_tags():
                if tag.get_tag_name() != "name":
                    tag_list = parsed_dict.get(tag.get_tag_name(), [])
                    tag_list.append(tag.get_tag_value())
                    parsed_dict[tag.get_tag_name()] = tag_list
                    if tag.get_tag_name() not in tag_names:
                        tag_names.append(tag.get_tag_name())
            parsed_table.append(parsed_dict)
        payload = jsonWriter(parsed_table)
        bulkurl = '/tagfiler/subject/name(%s)' % ';'.join([ urllib.quote(tag) for tag in tag_names ])
        connection = self._create_connection()
        login_cookie = self._login_request(connection)
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Cookie"] = login_cookie
        self.send_request("PUT", bulkurl, payload, headers)
        connection.close()
