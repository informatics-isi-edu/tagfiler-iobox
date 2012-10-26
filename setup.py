from distutils.core import setup
setup(name='tagfiler-iobox',
      version='0.1',
      description='Tagfiler IOBox',
      packages=['tagfiler', 'tagfiler.iobox', 'tagfiler.util', 'tagfiler.iobox.ui'],
      package_dir={'': 'src'},
      package_data={'tagfiler.iobox.ui': ['icons/*.gif'], 'tagfiler.iobox': ['sql/*.sql']},
      scripts=['bin/tagfiler-outbox', 'bin/tagfiler-outbox-systray']
      )
