import MySQLdb
import functools
import tornado.web

# TODO: Proper escaping for mysql

# Via http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
class memoized(object):
   """Decorator that caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned, and
   not re-evaluated.
   """
   
   def __init__(self, func):
      self.func = func
      self.cache = {}
      
   def __call__(self, *args):
      try:
         return self.cache[args]
      except KeyError:
         value = self.func(*args)
         self.cache[args] = value
         return value
      except TypeError:
         # uncachable -- for instance, passing a list as an argument.
         # Better to not cache than to blow up entirely.
         return self.func(*args)
     
   def __repr__(self):
      """Return the function's docstring."""
      return self.func.__doc__
  
   def __get__(self, obj, objtype):
      """Support instance methods."""
      return functools.partial(self.__call__, obj)
 
class DataInvalidError(tornado.web.HTTPError):
    def __init__(self, log_message=None):
        super(NotFoundError, self).__init__(400, log_message)
        
class NotFoundError(tornado.web.HTTPError):
    def __init__(self, log_message=None):
        super(NotFoundError, self).__init__(404, log_message)

class AlreadyExistsError(tornado.web.HTTPError):
    def __init__(self, log_message=None):
        super(AlreadyExistsError, self).__init__(409, log_message)

class InternalError(tornado.web.HTTPError):
    def __init__(self, log_message=None):
        super(InternalError, self).__init__(500, log_message)

class Interface:
    def list_databases(self):
        """
        return: []
        raise: InternalError
        """
        raise NotImplementedError()
    
    def list_tables(self, database):
        """
        return: []
        raise: InternalError
        """
        raise NotImplementedError()
    
    def list_record_keys(self, database, table):
        """
        return: []
        raise: InternalError
        """
        raise NotImplementedError()
    
    def get_record(self, database, table, record):
        """
        return: {}
        raise: NotFoundError 
        raise: InternalError
        """
        raise NotImplementedError()
    
    def delete_record(self, database, table, record):
        """
        return: None
        raise: NotFoundError
        raise: InternalError
        """
        raise NotImplementedError()
    
    def update_record(self, database, table, record, values):
        """
        return: []
        raise: NotFoundError
        raise: DataInvalidError
        raise: InternalError
        """
        raise NotImplementedError()
    
    def create_record(self, database, table, values):
        """
        return: []
        raise: DataInvalidError
        raise: AlreadyExistsError
        raise: InternalError        
        """
        raise NotImplementedError()    

class MySQL(Interface):
    
    def __init__(self, mysqldb):
        self.db = mysqldb
        
    @memoized
    def list_databases(self):
        try:
            cursor = self.db.cursor()
            num_databases = cursor.execute(
                'SELECT DISTINCT `table_schema` '
                'FROM            `information_schema`.`tables` '
            )
            
            if num_databases == 0:
                raise NotFoundError()
            
            return [database for (database,) in cursor.fetchall()]
        except MySQLdb.DatabaseError:
            raise InternalError()

    @memoized
    def list_tables(self, database):
        if database not in self.list_databases():
            raise NotFoundError()
        
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT DISTINCT `table_name` '
                'FROM            `information_schema`.`tables` '
                'WHERE           `table_schema` = "%s" '
                % (database,)
            )        
            return [table for (table,) in cursor.fetchall()]
        except MySQLdb.DatabaseError:
            raise InternalError()
    
    @memoized
    def _key_columns(self, database, table):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT `column_name`, `column_key` '
                'FROM   `information_schema`.`columns` '
                'WHERE  `table_schema` = "%s" '
                'AND    `table_name`   = "%s" ' 
                % (database, table)
            )        
            return [column for (column, key) in cursor.fetchall() if key is not '']
        except MySQLdb.DatabaseError:
            raise InternalError()

    def list_record_keys(self, database, table):
        try:
            sql_columns = '`, `'.join(self._key_columns(database, table))        
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT `%s` '
                'FROM   `%s`.`%s` ' 
                % (sql_columns, database, table)
            )        
            return cursor.fetchall()
        except MySQLdb.DatabaseError, e:
            code, message = e
            if code == 1146:
                raise NotFoundError()
            else: 
                raise InternalError()

    @memoized
    def _columns(self, database, table):
        try:            
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT `column_name` '
                'FROM   `information_schema`.`columns` '
                'WHERE  `table_schema` = "%s" '
                'AND    `table_name`   = "%s" ' 
                % (database, table)
            )
            return [column for (column,) in cursor.fetchall()]
        except MySQLdb.DatabaseError:
            raise InternalError()
    
    def get_record(self, database, table, record):
        colums = self._columns(database, table)
        sql_columns = '`, `'.join(colums)
        sql_where = ''
        
        for idx in xrange(len(record)):
            if sql_where is not '':
                sql_where += ' AND '                
            sql_where += '`%s` = "%s"' % (colums[idx], record[idx])
        
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT `%s` '
                'FROM   `%s`.`%s` '
                'WHERE  %s '
                'LIMIT  2 ' 
                % (sql_columns, database, table, sql_where)
            )
            records = cursor.fetchall()
        except MySQLdb.DatabaseError:
            raise InternalError()
        
        if len(records) == 0 or len(records) > 1:
            raise NotFoundError()
        
        # TODO: Is there a more pythonic way?
        record = {}
        for idx in xrange(len(colums)): 
            record[colums[idx]] = records[0][idx]
            
        return record

    def delete_record(self, database, table, record):
        # Rely on `get_record` to check wether the record exists or not
        self.get_record(database, table, record)
        
        colums = self._columns(database, table)
        sql_where = ''
        
        for idx in xrange(len(record)):
            if sql_where is not '':
                sql_where += ' AND '                
            sql_where += '`%s` = "%s"' % (colums[idx], record[idx])
        
        try:
            cursor = self.db.cursor()
            records_deleted = cursor.execute(
                'DELETE FROM `%s`.`%s` '
                'WHERE        %s '
                'LIMIT 1 ' 
                % (database, table, sql_where)
            )
            if records_deleted == 0:
                raise NotFoundError()
        except MySQLdb.DatabaseError:
            raise InternalError()

    def create_record(self, database, table, values):
        if values.keys() != self._columns(database, table):
            raise DataInvalidError() 

        columns = [key for key in values.keys()]
        insert_values = [str(value) for value in values.values()]
        
        sql_columns = '`, `'.join(columns)
        sql_values = '", "'.join(insert_values)
        
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'INSERT INTO `%s`.`%s` (`%s`) '
                'VALUES      ("%s") ' 
                % (database, table, sql_columns, sql_values)
            )
        except MySQLdb.DatabaseError, e:
            code, message = e
            if code == 1062:
                raise AlreadyExistsError()
            else: 
                raise InternalError()
        
        return [str(values[key]) for key in values if key in self._key_columns(database, table)]

    def update_record(self, database, table, record, values):
        columns = self._columns(database, table)
        
        if values.keys() != columns:
            raise DataInvalidError()         
        
        sql_where = ''
        for idx in xrange(len(record)):
            if sql_where is not '':
                sql_where += ' AND '
            sql_where += '`%s` = "%s"' % (columns[idx], record[idx])
            
        sql_set = ''
        for key in columns:
            if sql_set is not '':
                sql_set += ', '                                
            sql_set += '`%s` = "%s"' % (key, values[key])
                        
        try:
            cursor = self.db.cursor()
            rows_updated = cursor.execute(
                'UPDATE `%s`.`%s` '
                'SET     %s '
                'WHERE   %s ' 
                'LIMIT   1 '  
                % (database, table, sql_set, sql_where)
            )
        except MySQLdb.DatabaseError:
            raise AlreadyExistsError()
        
        return [str(values[key]) for key in values if key in self._key_columns(database, table)]