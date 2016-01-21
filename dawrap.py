#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :
"""arcpy data access function wrappers"""

import arcpy

__author__ = 'Edwin Sheldon @eas604'


def insert_row(table, fields, row):
    """
    Insert a single row into the table.
    :param table: table to insert intor
    :param fields: field names
    :param row: row collection
    """
    rows = arcpy.da.InsertCursor(table, fields)
    rows.insertRow(row)
    del row, rows, fields


def update_row(table, vals, where):
    """
    Updates a single row in the table with the values provided.
    :param table: table to insert into
    :param vals: value dictionary
    :param where: where clause
    """
    fields = vals.keys()
    cursor = arcpy.da.UpdateCursor(table, fields, where)
    for row in cursor:
        for key, val in vals.iteritems():
            row[fields.index(key)] = val

        cursor.updateRow(row)
        del cursor
        break


def update_rows(table, vals, where=None):
    """
    Update rows with provided values
    :param table: table to insert into
    :param vals: value dictionary
    :param where: where clause
    """
    fields = vals.keys()
    with arcpy.da.UpdateCursor(table, fields, where) as cursor:
        for row in cursor:
            for i, field in enumerate(fields):
                row[i] = vals[field]

            cursor.updateRow(row)
        del cursor


def retrieve_single_value(table, desired_col, where=None):
    """
    Query the table for a single column value from a single record matching a
    where clause.
    :param table: table name to be queried
    :param desired_col: column name to be queried
    :param where: where clause to use in query
    :return: a single value from the table matching the column name and where
    clause, or None
    """
    retval = None
    for row in get_rows(table, [desired_col], where):
        retval = row[0]
        del row
        break
    return retval


def get_rows(table, fields, where=None):
    """
    Return a generator to iterate over rows. Intended simply to factor this
    repetitive pattern from other functions.
    :param table: name of table, feature class, or shapefile
    :param fields: fields to search
    :param where: optional where clause of records to match
    :return: generator to iterate over table's rows
    """
    with arcpy.da.SearchCursor(table, fields, where) as cursor:
        for row in cursor:
            yield row


def clear_data(table, where):
    """
    Remove all rows from the table matching the expression in the where clause,
    returning a count of deleted records.
    :param table: name of table to search
    :param where: where clause to find records to delete
    :return: count of deleted rows
    :rtype int
    """
    count = 0
    with arcpy.da.UpdateCursor(table, ['ObjectID'], where) as cursor:
        for row in cursor:
            cursor.deleteRow()
            count += 1
            del row
        del cursor
    return count


def row_dict_from_table(table, where=None):
    """
    Given a table name, return a dictionary to represent fields and their
    values.
    :rtype : dict
    :param table: table name string
    :param where: where clause string
    """
    if where is None:
        where = u''

    assert isinstance(table, (unicode, str))
    assert isinstance(where, (unicode, str))

    # Get field names regardless of whether the set returned results
    fields = [f.name for f in arcpy.ListFields(table)]
    result = {f: None for f in fields}

    # Get their values, which will only be called if at least one row was
    # returned. Otherwise, values will remain None.
    for row in get_rows(table, fields, where):
        for i, f in enumerate(fields):
            result[f] = row[i]

    return result


def get_column_as_list(table, desired_col, where=None):
    """
    Given a table, desired column, and optional where clause, get a list of all
    matching values.
    :param table: table name to search
    :param desired_col: desired column
    :param where: optional where clause
    :return: list of matching values
    :rtype : list
    """
    for param in (table, desired_col, where):
        if param is not None:
            assert isinstance(param, (unicode, str))
    return [row[0] for row in get_rows(table, [desired_col], where)]


def copy_data(source_table, target_table, where=None, fields=None):
    """
    Copies field values from the source to the target table, matching the where
    clause.
    :param source_table: source table or feature class
    :param target_table: target table or feature class
    :param where: where clause of matching records
    :param fields: fields to copy
    :return: count of inserted records
    :rtype int
    """
    if fields is None:
        fields = '*'

    ins_count = 0
    for row in get_rows(source_table, fields, where):
        insert_row(target_table, fields, row)
        ins_count += 1
        del row
    return ins_count
