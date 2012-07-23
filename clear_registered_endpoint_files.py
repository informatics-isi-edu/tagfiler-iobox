'''
Created on Jul 23, 2012

@author: smithd
'''
import settings
from iobox import TagfilerServiceClient
import urllib
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

if __name__ == "__main__":
    client = TagfilerServiceClient(baseuri=settings.TAGFILER_BASEURL, username=settings.TAGFILER_USERNAME, password=settings.TAGFILER_PASSWORD)
    client.send_login_request()
    login_cookie = client.send_login_request()
    headers = {}
    headers["Content-Type"] = "application/json"
    headers["Cookie"] = login_cookie
    headers["Accept"] = "application/json"

    # get all of the files with the tagfiler ep in the name
    processing = True
    i = 0
    while processing is True:
        fetchurl = '/tagfiler/query/name:like:%s(id)' % urllib.quote("file://" + settings.TAGFILER_PEER_NAME + "/%", safe='')
        endpoint_files = jsonReader(client.send_request('GET', fetchurl, headers=headers).read())
        if len(endpoint_files) > 0:
            for f in endpoint_files:
                deleteurl = '/tagfiler/subject/id=%s' % f['id']
                resp = client.send_request('DELETE', deleteurl, headers=headers)
                i += 1
        else:
            processing = False
    print "Deleted %i entries from tagfiler" % i
