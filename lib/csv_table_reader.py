# Copyright (C) 2014 MediaMath, Inc. <http://www.mediamath.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import csv
import re

import logging

import qasino_table

class CsvTableReader(object):

    csv_to_qasino_type_map = { 'str' : 'TEXT',
                               'string' : 'TEXT',
                               'ip' : 'TEXT',
                               'varchar' : 'TEXT',
                               'float' : 'REAL',
                               'int' : 'INTEGER',
                               'integer' : 'INTEGER',
                               'll' : 'INTEGER',
                               'time' : 'INTEGER'
                               }

    def istrue(self, str):
        if str == None:
            return False

        m = re.search(r'^(false|no|0)$', str, flags=re.IGNORECASE)
        if m != None:
            return False

        return True


    def read_table(self, filehandle, tablename, 
                   colnames_lineno=1,
                   types_lineno=2,
                   options_lineno=-1,
                   tablename_lineno=-1,
                   skip_linenos=set()):

        table = qasino_table.QasinoTable(tablename)

        column_names = None
        column_types = None
    
        try:
            for lineno, line in enumerate(filehandle):

                line = line.rstrip('\n\r')

                if lineno in skip_linenos:
                    continue

                elif tablename_lineno == lineno:

                    table.set_tablename(line)

                elif options_lineno == lineno:

                    try:
                        parsed = csv.reader( [ line ] ).next()

                        for option_pair in parsed:

                            # If its a name value pair, its an option (otherwise its just a version number which we ignore)

                            m = re.search(r'^(\S+)=(\S+)$', option_pair, flags=re.IGNORECASE)
                            if m != None:
                                if m.group(1) == 'static' and self.istrue(m.group(2)):
                                    table.set_property('static', 1)
                                elif m.group(1) == 'update' and self.istrue(m.group(2)):
                                    table.set_property('update', 1)
                                elif m.group(1) == 'persist' and self.istrue(m.group(2)):
                                    table.set_property('persist', 1)
                                elif m.group(1) == 'keycols':
                                    table.set_property('keycols', m.group(2))
                                elif m.group(1) == 'identity':
                                    table.set_property('identity', m.group(2))

                    except Exception as inst:
                        raise Exception("Unable to parse options: %s" % (lineno + 1, inst))

                elif types_lineno == lineno:

                    try:
                        parsed = csv.reader( [ line ] ).next()
                        column_types = [ self.csv_to_qasino_type_map[x.strip()] for x in parsed ]
                    except Exception as inst:
                        raise Exception("Unsupported type in type list '%s' or parse error" % inst)
    
                    if column_names != None and len(column_names) != len(column_types):
                        raise Exception("Number of type names does not match number of column names! (line %d)" % lineno + 1)

                    table.set_column_types(column_types)

                elif colnames_lineno == lineno:

                    column_names = csv.reader( [ line ] ).next()
                
                    if column_types != None and len(column_names) != len(column_types):
                        raise Exception("Number of column names does not match number of column types! (line %d)" % lineno + 1)
    
                    table.set_column_names(column_names)

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
                            raise Exception("Could not find type for column %d on line %d!" % (column_index, lineno + 1))

                        try:
                            # Accept "null" fields as is (regardless of if they are an int/real etc)
                            if column_cell is None or column_cell == '':
                                output_row.append(column_cell)
                            elif column_type == 'INTEGER':
                                output_row.append(int(column_cell))
                            elif column_type == 'REAL':
                                output_row.append(float(column_cell))
                            else:
                                output_row.append(column_cell)
                        except Exception as e:
                            raise Exception("Parse error on line %d: %s" % (lineno + 1, str(e) ))
    
                    if table.add_row(output_row) == -1:
                        raise Exception("Wrong number of rows on line %d: '%s'" % (lineno + 1, line))
    
                # END if .. else .. 

            # END for each line

        except Exception as inst:
            #raise Exception("Csv read error on line %d" % lineno)
            logging.error('Csv read error on line %d: %s', lineno + 1, inst)
            return (None, str(inst))

        table.set_column_names(column_names)
        table.set_column_types(column_types)

        return ( table, None )
