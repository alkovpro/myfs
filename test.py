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
from model import MyFS

args = docopt(__doc__, version='0.0.1')

if type(args) is str:
    print(args)
    exit()

if args['mkdir']:
    MyFS.mkdir(args['<path>'])

elif args['touch']:
    MyFS.touch(args['<path>'])

elif args['list']:
    for fileitem in MyFS.list(args['<path>']):
        print (u' %s ' if fileitem.nodetype == 'File' else u'[%s]') % fileitem.name

elif args['remove']:
    MyFS.remove(args['<path>'], args['force'])

elif args['info']:
    info = MyFS.info(args['<path>'])
    print ("Info on '%s':" % args['<path>'])
    for k, v in info.iteritems():
        print (u'  %10s : %s' % (k, v))
