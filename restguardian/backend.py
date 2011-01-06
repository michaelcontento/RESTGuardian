import MySQLdb
import tornado.web

# TODO: Proper escaping for mysql 

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
        super(InternalError, self).__init__(501, log_message)

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
        
    def list_databases(self):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT DISTINCT `table_schema` '
                'FROM            `information_schema`.`tables` '
            )
            # TODO: Special NotFoundError so we can differ between NotFound and InternalError
            return [database for (database,) in cursor.fetchall()]
        except:
            raise InternalError()

    def list_tables(self, database):
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'SELECT DISTINCT `table_name` '
                'FROM            `information_schema`.`tables` '
                'WHERE           `table_schema` = "%s" '
                % (database,)
            )        
            # TODO: Special NotFoundError so we can differ between NotFound and InternalError
            return [table for (table,) in cursor.fetchall()]
        except:
            raise InternalError()
    
    def _key_columns(self, database, table):
        # TODO: Add caching for the result
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
        except:
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
        except Exception, e:
            code, message = e
            if code == 1146:
                raise NotFoundError()
            else: 
                raise InternalError()

    def _columns(self, database, table):
        # TODO: Add caching for the result
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
        except:
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
        except:
            raise InternalError()
        
        if len(records) == 0 or len(records) > 1:
            raise NotFoundError()
        
        # TODO: Is there a more pythonic way?
        record = {}
        for idx in xrange(len(colums)): 
            record[colums[idx]] = records[0][idx]
            
        return record

    def delete_record(self, database, table, record):
        colums = self._columns(database, table)
        sql_where = ''
        
        for idx in xrange(len(record)):
            if sql_where is not '':
                sql_where += ' AND '                
            sql_where += '`%s` = "%s"' % (colums[idx], record[idx])
        
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'DELETE FROM `%s`.`%s` '
                'WHERE        %s ' 
                % (database, table, sql_where)
            )
            # TODO: NotFound error if there is nothing to delete
        except:
            raise InternalError()

    def create_record(self, database, table, values):
        # TODO: Also check the datatypes for each column    
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
        except:
            # TODO: Raise special AleadyExistsError 
            raise InternalError()
        
        # TODO: Return the key for the record
        return []

    def update_record(self, database, table, record, values):
        # TODO: Also check the datatypes for each column    
        if values.keys() != self._columns(database, table):
            raise DataInvalidError()         
        
        columns = [key for key in values.keys()]
        insert_values = [str(value) for value in values.values()]
                        
        sql_columns = '`, `'.join(colums)
        sql_values = '", "'.join(insert_values)
                        
        try:
            cursor = self.db.cursor()
            cursor.execute(
                'REPLACE INTO `%s`.`%s` (`%s`) '
                'VALUES       ("%s") '  
                % (database, table, sql_columns, sql_values)
            )
            # TODO: Use UPDATE instead of REPLACE (new primary key!)
            # TODO: NotFoundError if there is nothing to update
        except:
            raise InternalError()
        
        # TODO: Return the key for the record
        return []
        
        
        
        
        
        
        
        
        
        