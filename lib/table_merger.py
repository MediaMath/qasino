
import logging

class TableMerger(object):

    def __init__(self, data_manager):
        self.data_manager = data_manager


    def merge_table(self, txn, tablename, table_to_add, existing_schema, sql_backend):

        # Determine if we need to merge this table.

        # Make some sets

        table_to_add_columns = set( table_to_add["column_names"] )
        existing_columns = set( [ row[0] for row in existing_schema ] )

        # Find discrepancies between table and existing schema.

        # First, are there new columns in the table to add?

        columns_to_add = table_to_add_columns - existing_columns

        if len(columns_to_add) > 0:

            # The existing table we are adding to is missing columns.
            # Try to add the missing columns to the table.

            column_type_lookup = { column_name : column_type 
                                   for column_name, column_type in zip(table_to_add["column_names"], table_to_add["column_types"]) }

            for column_name in columns_to_add:
                column_type = column_type_lookup[column_name]
                sql = "ALTER TABLE %s ADD COLUMN %s %s DEFAULT NULL;" % (tablename, column_name, column_type)

                logging.info("TableMerger: Altering table %s to add column %s %s", tablename, column_name, column_type)
                sql_backend.do_sql(txn, sql)


        # Else we may have columns that are missing from the table to
        # add (ie in the existing schema but not in the new table to
        # add).  We do nothing though because the insert will just not
        # insert those columns and we assume default values for all
        # columns.

        # TODO type changes.

