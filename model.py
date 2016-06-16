# # -*- coding: utf-8 -*-
import py2neo
import datetime

py2neo.authenticate('localhost:7474', 'neo4j', '12345')
graph = py2neo.Graph("http://localhost:7474/db/data")


def param(x, y, z):
    return x[y] if y in x else z


class FileNotFound(Exception):
    def __init__(self, msg):
        self.message = msg


class FileExists(Exception):
    def __init__(self, msg):
        self.message = msg


class WriteDataError(Exception):
    def __init__(self, msg):
        self.message = msg


# TODO: Add users and concurrency
# TODO: Add special symbols in path. For example '\/' -> '/'.
# TODO: Add "Property" class and it's descendants for each property type: StringProperty(default='', required=True)
# TODO: Add timezones
# TODO: Add __doc__

class FSNode(object):
    nodetype = ''
    parent = None
    mode = 0777
    owner = None
    id = None
    name = None
    created = None
    modified = None

    def __init__(self, **kwargs):
        noload = param(kwargs, 'noload', False)
        self.map(**kwargs)
        if not noload:
            self.load()

    def load(self):
        query = u''
        if self.id is not None:
            query = u'MATCH (f%s{id:{ID}}) RETURN f' % (':%s' % self.nodetype) if len(self.nodetype) > 0 else ''
        elif self.name is not None:
            if self.parent is None:
                query = u'MATCH (f:Folder{root:true}) RETURN f'
            else:
                query = u'MATCH (f%s{parent:{PARENT},name:{NAME}}) RETURN f' % (':%s' % self.nodetype) if len(
                    self.nodetype) > 0 else ''

        match = graph.cypher.execute(query, ID=self.id, PARENT=self.parent, NAME=self.name)
        if len(match) > 0:
            match = getattr(getattr(match[0], 'f'), 'properties')
            self.map(**match)

    def map(self, **kwargs):
        """ maps keyword arguments to the fsNode's properties """
        for k, v in kwargs.iteritems():
            # class has attribute?
            if hasattr(self.__class__, k):
                setattr(self, k, v)

    def save(self):
        self.modified = datetime.datetime.now()
        if self.created is None:
            self.created = self.modified
        params = u''
        values = {}
        counter = 0
        for k, v in self.__dict__.iteritems():
            if k not in ['nodetype', 'parent']:
                if type(v) in [int, float, str, datetime.datetime, bool]:
                    counter += 1
                    params += u'f.%s = {v%i}, ' % (k, counter)
                    values['v%i' % counter] = v
        params = params[:-2]
        if self.id is not None:
            values['ID'] = self.id
            query = u'MATCH (f:%s{id:{ID}}) SET %s' % (self.nodetype, params)
        else:
            if self.parent is None:
                self.parent = FolderNode(root=True).id
            values['ID'] = self.parent
            query = u'MATCH (p:Folder{id:{ID}}) CREATE (f:%s)-[r:`PARENT`]->(p) SET f.id=id(f),%s RETURN id(f) as id' \
                    % (self.nodetype, params)
        matches = graph.cypher.execute(query, **values)
        if len(matches) > 0:
            self.id = getattr(matches[0], 'id')

    def remove(self, force):
        query = u'MATCH (f{id:{ID}}) OPTIONAL MATCH (f)<-[*]-(f1:Folder) OPTIONAL MATCH (f)<-[*]-(f2:File) ' \
                u'RETURN [f1.id,f2.id] as children'
        children = []
        matches = graph.cypher.execute(query, ID=self.id)
        for match in matches:
            children.extend(getattr(match, 'children'))
        children = list(set(children))
        if None in children:
            children.remove(None)
        if len(children) > 0:
            if not force:
                raise FileExists('Folder has child items')
        children.append(self.id)
        query = u'MATCH (f) WHERE f.id in %s DETACH DELETE f' % unicode(children)
        try:
            graph.cypher.execute(query)
        except:
            raise WriteDataError("Can't delete '%s' %s" % (self.name, self.nodetype.lower()))

    def info(self, _reload=False):
        if _reload:
            self.load()
        return self.__dict__


class FolderNode(FSNode):
    nodetype = 'Folder'
    root = False

    def __init__(self, **kwargs):
        root = param(kwargs, 'root', False)
        name = param(kwargs, 'name', '')
        if root or name == '/':
            query = u'MATCH (f:Folder{root:true}) RETURN id(f) as id,f.name LIMIT 1'
            matches = graph.cypher.execute(query, ID=self.id)
            if len(matches) > 0:
                self.id = getattr(matches[0], 'id')
                self.name = getattr(matches[0], 'f.name')
            else:
                self.created = datetime.datetime.now()
                self.modified = self.created
                query = u'CREATE (f:Folder{root:true,name:{NAME},created:{CREATED},modified:{CREATED}}) ' \
                        u'SET f.id=id(f) RETURN id(f) as id'
                matches = graph.cypher.execute(query, NAME='/', CREATED=self.created)
                if len(matches) > 0:
                    self.id = getattr(matches[0], 'id')
                    self.name = u'/'
                else:
                    raise WriteDataError("Can't create root folder")
        else:
            kwargs['nodetype'] = self.nodetype
            super(FolderNode, self).__init__(**kwargs)

    def list(self):
        result = []
        query = u'MATCH (f)-[r:`PARENT`]->(Folder{id:{ID}}) RETURN id(f) as id,f.name,labels(f)[0] AS nodetype ' \
                u'ORDER BY nodetype DESC'
        matches = graph.cypher.execute(query, ID=self.id)
        for match in matches:
            params = {
                'id': getattr(match, 'id'),
                'name': getattr(match, 'f.name'),
                'noload': True,
            }
            nodetype = getattr(match, 'nodetype')
            result.append(FolderNode(**params) if nodetype == 'Folder' else FileNode(**params))
        return result


class FileNode(FSNode):
    nodetype = 'File'
    checksum = None
    size = None

    def __init__(self, **kwargs):
        kwargs['nodetype'] = self.nodetype
        super(FileNode, self).__init__(**kwargs)


class MyFS(object):
    @staticmethod
    def get_nodes_for_path(path=''):
        # Check if root folder exists
        FolderNode(root=True)
        name = u''
        counter = 1
        query = u'MATCH (f1:Folder{root:true}) '
        params = {}
        query_end = [{
            'id': 'f1.id',
            'name': 'f1.name',
            'nodetype': 'labels(f1)[0]',
        }]
        if path not in ['', '/']:
            crumbs = path.split('/')
            if crumbs[0] == '':
                crumbs = crumbs[1:]  # leading slash
            if crumbs[-1] == '':
                crumbs = crumbs[:-1]  # ending slash
            for crumb in crumbs:
                counter += 1
                params['cr%i' % counter] = crumb
                query += u'OPTIONAL MATCH (f%i)<-[r%i:`PARENT`]-(f%i{name:{cr%i}}) ' \
                         % (counter - 1, counter - 1, counter, counter)
                query_end.append({
                    'id': 'f%i.id' % counter,
                    'name': 'f%i.name' % counter,
                    'nodetype': 'labels(f%i)[0]' % counter,
                })
                name = crumb
        if len(query_end) > 2:
            query_end = query_end[-2:]
        query_end = unicode(query_end).replace("'", '')
        query += u'RETURN %s as path' % query_end
        match = graph.cypher.execute(query, **params)
        node = None
        parent = None
        if len(match) > 0:
            match = getattr(match[0], 'path')
            if len(match) == 1:
                if match[0]['id'] is not None:
                    match[0]['noload'] = True
                    node = FolderNode(**match[0]) if match[0]['nodetype'] == 'Folder' else FileNode(**match[0])
            else:
                if match[1]['id'] is not None:
                    match[1]['noload'] = True
                    node = FolderNode(**match[1]) if match[1]['nodetype'] == 'Folder' else FileNode(**match[1])
                if match[0]['id'] is not None:
                    match[0]['noload'] = True
                    parent = FolderNode(**match[0])
        return node, parent, name

    @staticmethod
    def list(path):
        (node, parent, name) = MyFS.get_nodes_for_path(path)
        if node is None:
            raise FileNotFound('Path not found')
        if node.nodetype == 'File':
            result = [node]
        else:
            result = node.list()
        return result

    @staticmethod
    def mkdir(path):
        (node, parent, name) = MyFS.get_nodes_for_path(path)
        if node is not None:
            raise FileExists('%s already exists' % node.nodetype)
        if parent is None:
            raise FileNotFound('Path not found')
        node = FolderNode(parent=parent.id, name=name)
        node.save()
        return node

    @staticmethod
    def touch(path):
        (node, parent, name) = MyFS.get_nodes_for_path(path)
        if node is not None:
            raise FileExists('%s already exists' % node.nodetype)
        if parent is None:
            raise FileNotFound('Path not found')
        node = FileNode(parent=parent.id, name=name, checksum=0)
        node.save()
        return node

    @staticmethod
    def info(path):
        (node, parent, name) = MyFS.get_nodes_for_path(path)
        if node is None:
            raise FileNotFound('Path not found')
        return node.info(True)

    @staticmethod
    def remove(path, force):
        (node, parent, name) = MyFS.get_nodes_for_path(path)
        if node is None:
            raise FileNotFound('Path not found')
        node.remove(force)
