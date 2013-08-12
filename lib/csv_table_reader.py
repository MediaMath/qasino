
import sys
import csv

import logging

class CsvTableReader(object):

    csv_to_qasino_type_map = { 'str' : 'TEXT',
                               'string' : 'TEXT',
                               'ip' : 'TEXT',
                               'int' : 'INTEGER',
                               'integer' : 'INTEGER',
                               'll' : 'INTEGER',
                               'time' : 'INTEGER'
                               }

    def read_table(self, filename, tablename, 
                   colnames_lineno=1,
                   types_lineno=2,
                   skip_linenos=set()):

        # Open the file.
    
        try:
            file = open(filename, 'r')
        except Exception as e:
            logging.error("Failed to open file '%s': %s", filename, e)
            return
    
        column_names = None
        column_types = None
        output_rows = list()
    
        try:
            for lineno, line in enumerate(file):

                line = line.rstrip('\n\r')

                if lineno in skip_linenos:
                    continue

                elif types_lineno and types_lineno == lineno:

                    try:
                        parsed = csv.reader( [ line ] ).next()
                        column_types = [ self.csv_to_qasino_type_map[x] for x in parsed ]
                    except Exception as inst:
                        raise Exception("Unsupported type in type list '%s' or parse error" % inst)
    
                    if column_names and len(column_names) != len(column_types):
                        raise Exception("Number of type names does not match number of column names! (line %d)" % lineno)

                elif colnames_lineno and colnames_lineno == lineno:

                    column_names = csv.reader( [ line ] ).next()
                
                    if column_types and len(column_names) != len(column_types):
                        raise Exception("Number of column names does not match number of column types! (line %d)" % lineno)
    
                # Data
                else:

                    input_row = csv.reader( [ line ] ).next()

                    ## Parse all the data into rows.
    
                    output_row = list()
    
                    # Read each column in data.

                    for column_index, column_cell in enumerate(input_row):
    
                        ##print "%d CELL: %s  name: %s" % (column_index, column_cell, column_names[column_index])
    
                        # Get the type so we know if it should be an int or not
    
                        try:
                            column_type = column_types[column_index]
                        except:
                            raise Exception("Could not find type for column %d on line %d!" % (column_index, lineno))

                        try:
                            if (column_type == 'int'):
                                output_row.append(int(column_cell))
                            else:
                                output_row.append(column_cell)
                        except:
                            raise Exception("Parse error on line %d" % lineno)
    
                    output_rows.append(output_row)
    
                # END if .. else .. 

            # END for each line

        except Exception as inst:
            #raise Exception("Csv read error on line %d" % lineno)
            logging.error('Csv read error on line %d: %s', lineno, inst)
            return

        return { "tablename" : tablename,
                 "column_names" : column_names,
                 "column_types" : column_types,
                 "rows" : output_rows 
               }
