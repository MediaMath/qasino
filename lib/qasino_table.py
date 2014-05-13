
import json
import random

import util

class QasinoTable(object):
    """
    A simple container for a qasino table.
    """

    def __init__(self, tablename=None):
        self.tablename = tablename
        self.rows = []
        self.column_names = []
        self.column_types = []
        self.properties = {}

    def zip_columns(self):
        return zip(self.column_names, self.column_types)

    def set_tablename(self, tablename):
        self.tablename = tablename

    def get_tablename(self):
        return self.tablename

    def get_column_names(self):
        return self.column_names

    def init_retry(self, nr_retries=5):
        if self.__dict__.has_key('retry_count'):
            return
        self.retry_count = nr_retries

    def test_retry(self):
        try:
            self.retry_count -= 1
            if self.retry_count <= 0:
                return True
        except:
            pass

        return False

    def get_obj(self, **extra_settings):
        obj = {}
        for name, value in self.properties.iteritems():
            obj[name] = value

        for name, value in extra_settings.iteritems():
            obj[name] = value

        obj["table"] = { "tablename" : self.tablename,
                         "column_names" : self.column_names,
                         "column_types" : self.column_types,
                         "rows" : self.rows
                        }
        return obj

    def get_json(self, **extra_settings):
        return json.dumps(self.get_obj(**extra_settings))

    def get_rows(self):
        return self.rows

    def get_row(self, row_index):
        try:
            return self.rows[row_index]
        except:
            pass
        return []

    def from_obj(self, obj):
        try:
            self.tablename = obj['table']['tablename']
            self.column_names = obj['table']['column_names']
            self.column_types = obj['table']['column_types']
            self.rows = obj['table']['rows']

            for key, value in obj.iteritems():
                if key != "table" and key != "op":
                    self.properties[key] = value
        except:
            return -1

        return 1

    def printit(self):
        print self.get_obj()

    def get_nr_rows(self):
        return len(self.rows)

    def set_property(self, property_name, property_value):
        self.properties[property_name] = property_value

    def get_property(self, property_name):
        if property_name in self.properties:
            return self.properties[property_name]
        else:
            return None

    def set_column_names(self, column_names):
        self.column_names = column_names

    def set_column_types(self, column_types):
        self.column_types = column_types

    def add_column(self, colname, coltype):
        self.column_names.append(colname)
        self.column_types.append(coltype)

    def add_row(self, row):
        if len(row) != len(self.column_names) or len(row) != len(self.column_types):
            return -1
        self.rows.append(row)
        return 1

    def get_nr_column_names(self):
        return len(self.column_names)

    def get_nr_column_type(self):
        return len(self.column_types)



def get_a_random_table():

    type_array = [ "TEXT", "INTEGER", "REAL" ]

    nr_columns = random.randint(1, 20)

    column_types = [ type_array[ random.randint(0, len(type_array) - 1) ] for _ in range(nr_columns) ]

    table = QasinoTable(util.random_string(4, 20))
    table.set_column_names( [ util.random_string(1, 40) for _ in range(nr_columns) ] )
    table.set_column_types( column_types )

    rows = []
    for row_index in range(random.randint(1, 300)):
        row = []
        for column_index in range(nr_columns):
            if column_types[column_index] == "TEXT":
                row.append(util.random_string(1, 50))
            elif column_types[column_index] == "REAL":
                row.append(random.randint(0, 3483839392) / 100.0)
            else:
                row.append(random.randint(0, 3483839392))
        table.add_row(row)

    return table
