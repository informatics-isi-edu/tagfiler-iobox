from distutils.core import setup
setup(name='tagfiler-iobox',
      version='0.1',
      package_dir={'tagfiler': 'src/tagfiler'},
      packages=['tagfiler', 'tagfiler.iobox', 'tagfiler.util', 'tagfiler.common'],
      )
