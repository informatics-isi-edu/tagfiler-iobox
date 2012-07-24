# date=2012-01-20
# session=session1
# name=file://smithd#tagfiler_ep/opt/data/studies/2012-01-20/session1/
def generate_rules(endpoint_name):
    return (
         ('pathmatch', 
           {'pattern': '^/.*/studies/([^/]+)/([^/]+)/', 'extract': 'positional', 'tags':['date', 'session']}
         ),
         ('pathmatch',
           {'pattern': '^(?P<path>.*)',
            'templates': ['file://%s\g<path>' % endpoint_name],
            'extract': 'template',
            'tags': ['name'] }
		 )
    )

