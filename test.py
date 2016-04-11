#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Usage:
    test mkdir <path>
    test touch <path>
    test list <path>
    test [force] remove <path>
    test info <path>
"""

from docopt import docopt
from model import fileNode,folderNode,myfs

args=docopt(__doc__, version='0.0.1')

if type(args) is str:
    print(args)
    exit()

if args['mkdir']:
    myfs.mkdir(args['<path>'])

elif args['touch']:
    myfs.touch(args['<path>'])

elif args['list']:
    for fileitem in myfs.list(args['<path>']):
        print (u' %s ' if fileitem.nodetype=='File' else u'[%s]') % fileitem.name

elif args['remove']:
    myfs.remove(args['<path>'], args['force'])

elif args['info']:
    info = myfs.info(args['<path>'])
    print ("Info on '%s':" % args['<path>'])
    for k,v in info.iteritems():
        print (u'  %10s : %s' % (k, v))

