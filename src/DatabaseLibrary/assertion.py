#  Copyright (c) 2010 Franz Allan Valencia See
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Modified by Astun Technology

from robot.api import logger

class Assertion(object):
    """
    Assertion handles all the assertions of Database Library.
    """

    def query_should_return_rows(self, selectStatement):
        """
        Checks that something gets returned by `selectStatement`

        If there are no results, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you have the following assertions in your robot
        | Query Should Return Rows | select id from person where first_name = 'Franz Allan' |
        | Query Should Return Rows | select id from person where first_name = 'John' |

        Then you will get the following:
        | Query Should Return Rows | select id from person where first_name = 'Franz Allan' | # PASS |
        | Query Should Return Rows | select id from person where first_name = 'John' | # FAIL |
        """
        if not self.query(selectStatement):
            raise AssertionError("Expected to have have at least one row from '%s' "
                                 "but got 0 rows." % selectStatement)


    def query_should_not_return_rows(self, selectStatement):
        """
        Checks that result of `selectStatement` is empty

        If it isn't, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you have the following assertions in your robot
        | Query Should Not Return Rows | select id from person where first_name = 'Franz Allan' |
        | Query Should Not Return Rows | select id from person where first_name = 'John' |

        Then you will get the following:
        | Query Should Not Return Rows | select id from person where first_name = 'Franz Allan' | # FAIL |
        | Query Should Not Return Rows | select id from person where first_name = 'John' | # PASS |
        """
        results, count, __ = self._run_query(selectStatement)
        if (count > 0):
            logger.info('Query Should Not Return Rows actually returned {} rows:\n'
                        '{}'.format(count, results))
            raise AssertionError('Returned {} results when none expected from '
                                 '`{}`. '.format(count, selectStatement))


    def query_rows_should_equal(self, selectStatement, numRows):
        """
        Checks that rows in `selectStatement` results are equal to `numRows`

        If not, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Query Rows Should Equal | select id from person | 1 |
        | Query Rows Should Equal | select id from person where first_name = 'John' | 0 |

        Then you will get the following:
        | Query Rows Should Equal | select id from person | 1 | # FAIL |
        | Query Rows Should Equal | select id from person where first_name = 'John' | 0 | # PASS |
        """
        num_rows = self.count_rows_in_query(selectStatement)
        if (num_rows != int(numRows.encode('ascii'))):
            raise AssertionError("Expected same number of rows to be returned from '%s' "
                                 "to be %s" % (selectStatement, num_rows))

    def query_rows_should_be_more(self, selectStatement, numRows):
        """
        Checks that rows in `selectStatement` results are more than `numRows`

        If not, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Query Rows Should Be More | select id from person | 1 |
        | Query Rows Should Be More | select id from person where first_name = 'John' | 0 |

        Then you will get the following:
        | Query Rows Should Be More | select id from person | 1 | # PASS |
        | Query Rows Should Be More | select id from person where first_name = 'John' | 0 | # FAIL |
        """
        num_rows = self.count_rows_in_query(selectStatement)
        if (num_rows <= int(numRows.encode('ascii'))):
            raise AssertionError("Expected more rows to be returned from '%s' "
                                 "than the returned count of %s" % (selectStatement, num_rows))

    def query_rows_should_be_fewer(self, selectStatement, numRows):
        """
        Checks that rows in `selectStatement` results are fewer than `numRows`

        If not, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Query Rows Should Be Fewer | select id from person | 3 |
        | Query Rows Should Be Fewer | select id from person where first_name = 'John' | 1 |

        Then you will get the following:
        | Query Rows Should Be Fewer | select id from person | 3 | # PASS |
        | Query Rows Should Be Fewer | select id from person where first_name = 'John' | 1 | # FAIL |
        """
        num_rows = self.count_rows_in_query(selectStatement)
        if (num_rows >= int(numRows.encode('ascii'))):
            raise AssertionError("Expected fewer rows to be returned from '%s' "
                                 "than the returned count of %s" % (selectStatement, num_rows))



    def table_must_exist(self, tableName):
        """
        Check if the table given exists in the database.

        For example, given we have a table `person` in a database

        When you do the following:
        | Table Must Exist | person |

        Then you will get the following:
        | Table Must Exist | person | # PASS |
        | Table Must Exist | first_name | # FAIL |
        """
        selectStatement = ("select * from information_schema.tables where table_name='%s'" % tableName)
        num_rows = self.count_rows_in_query(selectStatement)
        if (num_rows == 0):
            raise AssertionError("Table '%s' does not exist in the database" % tableName)


    def table_must_not_exist(self, tableName):
        """
        Check if the table given does not exist in the database

        For example, given we have a table `person` in a database

        When you do the following:
        | Table Must Not Exist | person |

        Then you will get the following:
        | Table Must Not Exist | person | # FAIL |
        | Table Must Not Exist | first_name | # PASS |
        """
        selectStatement = ("select * from information_schema.tables where table_name='%s'" % tableName)
        num_rows = self.count_rows_in_query(selectStatement)
        if (num_rows > 0):
            raise AssertionError("Table '%s' exists in the database" % tableName)


    def table_rows_should_equal(self, tableName, numRows, schema='public'):
        """
        Checks that rows in `tableName` results are equal to `numRows`

        If not, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Table Rows Should Equal | person | 2 |
        | Table Rows Should Equal | person | 1 |

        Then you will get the following:
        | Table Rows Should Equal | person | 2 | # PASS |
        | Table Rows Should Equal | person | 1 | # FAIL |
        """
        num_rows = self.count_rows_in_table(tableName, schema=schema)
        if (num_rows != int(numRows.encode('ascii'))):
            raise AssertionError('Expected %s rows to be returned from "%s".%s" '
                                 'not the returned count of %s' % (numRows, schema, tableName, num_rows))


    def table_rows_should_be_more(self, tableName, numRows, schema='public'):
        """
        Checks that rows in `tableName` results are more than `numRows`

        If not, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your robot
        | Table Rows Should Be More | person | 1 |
        | Table Rows Should Be More | person | 2 |

        Then you will get the following:
        | Table Rows Should Be More | select id from person | 1 | # PASS |
        | Table Rows Should Be More | select id from person | 2 | # FAIL |
        """
        num_rows = self.count_rows_in_table(tableName, schema=schema)
        if (num_rows <= int(numRows.encode('ascii'))):
            raise AssertionError('Expected more than %s rows to be returned '
                                 'from "%s"."%s".\nThe returned count was %s' %
                                 (numRows, schema, tableName, num_rows))


    def table_rows_should_be_fewer(self, tableName, numRows, schema='public'):
        """
        Checks that rows in `tableName` results are fewer than `numRows`

        If not, then this will throw an AssertionError.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your test:
        | Table Rows Should Be Fewer | person | 3 |
        | Table Rows Should Be Fewer | person | 2 |

        Then you will get the following:
        | Table Rows Should Be Fewer | person | 3 | # PASS |
        | Table Rows Should Be Fewer | person | 2 | # FAIL |
        """
        num_rows = self.count_rows_in_table(tableName, schema=schema)
        if (num_rows >= int(numRows.encode('ascii'))):
            raise AssertionError('Expected fewer than %s rows to be returned '
                                 'from "%s"."%s".\nThe returned count was %s' %
                                 (numRows, schema, tableName, num_rows))


    def table_must_have_column(self, tableName, columnName, schema='public'):
        """
        Checks that the `tableName` has column `columnName`

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your test:
        | Table Must Have Column | person | first_name |
        | Table Must Have Column | person | surname    |

        Then you should get the following:
        | Table Must Have Column | person | first_name | #FAIL |
        | Table Must Have Column | person | surname    | #PASS |

        """
        sql = '''
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = '%s'
        AND table_name='%s'
        AND column_name='%s';
        ''' % (schema, tableName, columnName)
        count = self.count_rows_in_query(sql)
        if (count == 0):
            raise AssertionError('Column "%s" not found in "%s"."%s"'
                                 ) % (columnName, schema, tableName)
        if (count > 1):
            raise AssertionError('More than one column "%s" found in "%s"."%s"'
                                 ) % (columnName, schema, tableName)



    def table_must_have_columns(self, tableName, columnNames, schema='public'):
        """
        `tableName` must have all the columns in the `columnNames` list

        `columnNames` must be a list of column names, separated by commas.


        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you have the following assertions in your test:
        | Table Must Have Columns | person | id, first_name      |
        | Table Must Have Columns | person | first_name, surname |

        Then you should get the following:
        | Table Must Have Columns | person | id, first_name      | #FAIL |
        | Table Must Have Columns | person | first_name, surname | #PASS |

        """
        columns = [c.strip() for c in columnNames.split(',')]

        sql = '''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '%s'
        AND table_name='%s'
        AND column_name IN ('%s');
        ''' % (schema, tableName, "', '".join(columns))
        results, count, __ = self._run_query(sql)
        expected = len(columns)
        if (count != expected):
            raise AssertionError('Columns (%s) not all found in "%s"."%s".'
            'Columns returned: %s') % (columnNames, schema, tableName,
                                       ', '.join(results[0]))

    def table_column_must_be_unique(self, tableName, columnName, schema='public'):
        """
        """
        pass


