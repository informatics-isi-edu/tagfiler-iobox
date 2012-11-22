from distutils.core import setup
setup(name='tagfiler-outbox',
      version='0.0',
      description='Tagfiler Outbox',
      packages=['tagfiler', 'tagfiler.iobox', 'tagfiler.util'],
      package_dir={'': 'src'},
      package_data={'tagfiler.iobox': ['sql/*.sql']},
      scripts=['bin/tagfiler-outbox']
      )
