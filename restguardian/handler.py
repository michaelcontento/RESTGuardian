import json
import tornado.httpserver
import tornado.ioloop
import tornado.web
import MySQLdb

from restguardian import backend      
         
class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, backend):
        self.backend = backend
    
    def _url(self, database=None, table=None, record=[]):
        if not database:
            return '/'
        
        if not table:
            return '/%s' % (database,)
         
        url_record = '-'.join([str(value) for value in record])
        return '/%s/%s/%s' % (database, table, url_record)

    def _execute(self, transforms, *args, **kwargs):
        if len(args) >= 3:
            # TODO: A more pythonic way?
            args = [value for value in args]
            args[2] = args[2].split('-')
            args = tuple(args)
            
        super(BaseHandler, self)._execute(transforms, *args, **kwargs)

    def write(self, chunk):
        super(BaseHandler, self).write(json.dumps(chunk))

class RootHandler(BaseHandler):
    def get(self):
        databases = self.backend.list_databases()
        self.write([self._url(database) for database in databases])
    
class DatabaseHandler(BaseHandler):
    def get(self, database):
        if not database:
            self.send_errror(501)
            
        # TODO: Add real favicon and remove this hack
        if database == 'favicon.ico':
            return
        
        tables = self.backend.list_tables(database)
        self.write([self._url(database, table) for table in tables])
    
class TableHandler(BaseHandler):
    def get(self, database, table):
        if not database or not table:
            self.send_errror(501)
         
        records = self.backend.list_record_keys(database, table)
        self.write([self._url(database, table, record) for record in records])

    def post(self, database, table):
        if not database or not table or not self.request.body:
            self.send_errror(501)
                    
        json_body = json.loads(self.request.body)
        record = self.backend.create_record(database, table, json_body)
        
        self.set_header('Location', self._url(database, table, record))        
    
class RecordHandler(BaseHandler):
    def delete(self, database, table, record):
        if not database or not table or not record:
            self.send_errror(501)
        
        self.backend.delete_record(database, table, record)
    
    def get(self, database, table, record):
        if not database or not table or not record:
            self.send_errror(501)
        
        self.write(self.backend.get_record(database, table, record))
    
    def put(self, database, table, record):
        if not database or not table or not record or not self.request.body:
            self.send_errror(501)
        
        json_body = json.loads(self.request.body)
        new_record = self.backend.update_record(database, table, 
                                                record, json_body)            

        if new_record != record:
            self.set_header('Location', self._url(database, table, new_record))
        
def start(config_file, opts):
    try:
        settings = dict(backend=eval(open(config_file).read()))
    except Exception, e:
        print "error: something went wrong while reading the config file"
        print "\n%s" % e
        return
        
    urls = [
        (r'/', RootHandler, settings),
        (r'/([^/]+)/?', DatabaseHandler, settings),
        (r'/([^/]+)/([^/]+)/?', TableHandler, settings),
        (r'/([^/]+)/([^/]+)/([^/]+)/?', RecordHandler, settings),
    ]
    
    application = tornado.web.Application(urls)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(opts.port)
    tornado.ioloop.IOLoop.instance().start()        
