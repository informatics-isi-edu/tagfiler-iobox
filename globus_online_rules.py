# date=2012-01-20
# session=session1
# name=file://smithd#tagfiler_ep/opt/data/studies/2012-01-20/session1/

globus_online_rules = (
     ('pathmatch', 
           {'pattern': '^/opt/data/studies/([^/]+)/([^/]+)/', 'extract': 'positional', 'tags':['date', 'session']}),
     ('pathmatch',
           {'pattern': '^(?P<path>.*)',
            'templates': ['file://smithd#tagfiler_ep\g<path>'],
            'extract': 'template',
            'tags': ['name'] })
)

