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


class Query(object):
    """
    Query handles all the querying done by the Database Library.
    """

    def _run_query(self, selectStatement):
        cur = None
        try:
            cur = self._dbconnection.cursor()
            self.__execute_sql(cur, selectStatement)
            return cur.fetchall(), cur.rowcount, cur.description
        finally :
            if cur :
                self._dbconnection.rollback()

    def query(self, selectStatement):
        """
        Uses the input `selectStatement` to query for the values that
        will be returned as a list of tuples.

        Tip: Unless you want to log all column values of the specified rows,
        try specifying the column names in your select statements
        as much as possible to prevent any unnecessary surprises with schema
        changes and to easily see what your [] indexing is trying to retrieve
        (i.e. instead of `"select * from my_table"`, try
        `"select id, col_1, col_2 from my_table"`).

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you do the following:
        | @{queryResults} | Query | select * from person |
        | Log Many | @{queryResults} |

        You will get the following:
        [1, 'Franz Allan', 'See']

        Also, you can do something like this:
        | ${queryResults} | Query | select first_name, last_name from person |
        | Log | ${queryResults[0][1]}, ${queryResults[0][0]} |

        And get the following
        See, Franz Allan
        """
        results, __, __ = self._run_query(selectStatement)
        return results


    def call_function(self, functionName, *args):
        """
        Calls function `functionName` with any extra arguments and returns
        the result. This is not intended for use with functions that return a
        recordset or table (use `query` for those).

        For example, calling the standard `version` function in PostgreSQL:
        | ${ver} | Call Function | version |
        | Log | ${ver} |

        You will get something like:
        9.2.6
        """
        arguments = ''
        if args:
            arguments = ', '.join(args)

        selectStatement = 'SELECT {}({});'.format(functionName, arguments)
        return self._get_single_result(selectStatement)


    def count_rows_in_query(self, selectStatement):
        """
        Uses the input `selectStatement` to query the database and returns
        the number of rows from the query.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you do the following:
        | ${rowCount} | Count Rows In Query | select * from person |
        | Log | ${rowCount} |

        You will get the following:
        2

        Also, you can do something like this:
        | ${rowCount} | Count Rows In Query | select * from person where id = 2 |
        | Log | ${rowCount} |

        And get the following
        1
        """
        __, count, __ = self._run_query(selectStatement)
        return count


    def count_rows_in_table(self, table, schema='public'):
        """
        Uses the input `table` to query the database and returns
        the number of rows in the table.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |
        |  2 | Jerry       | Schneider |

        When you do the following:
        | ${rowCount} | Count Rows In Table | person |
        | Log | ${rowCount} |

        You will get the following:
        2

        """
        cur = None
        selectStatement = 'SELECT count(*) FROM "{}"."{}";'.format(schema,
                                                                   table)
        return self._get_single_result(selectStatement)


    def describe_data(self, selectStatement):
        """
        Describes the columns in the dataset resulting from `selectStatement`.

        *Note*: ` LIMIT 0` will be appended to the provided statement.

        For example, given we have a table `person` with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you do the following:
        | @{queryResults} | Describe Data | select * from person |
        | Log Many | @{queryResults} |

        You will get the following:
        [Column(name='id', type_code=1043, display_size=None, internal_size=255, precision=None, scale=None, null_ok=None)]
        [Column(name='first_name', type_code=1043, display_size=None, internal_size=255, precision=None, scale=None, null_ok=None)]
        [Column(name='last_name', type_code=1043, display_size=None, internal_size=255, precision=None, scale=None, null_ok=None)]
        """
        cur = None
        if selectStatement.endswith(';'):
            selectStatement = selectStatement.rstrip(';')
        selectStatement = '{} LIMIT 0;'.format(selectStatement)

        __, __, description = self._run_query(selectStatement)
        return description


    def describe_table(self, table, schema='public'):
        """
        Describes the columns in (`schema`.)`table`

        For example, given we have a table _person_ with the following data:
        | id | first_name  | last_name |
        |  1 | Franz Allan | See       |

        When you do the following:
        | @{queryResults} | Describe Table | person |
        | Log Many | @{queryResults} |

        You will get the following:
        [Column(name='id', type_code=1043, display_size=None, internal_size=255, precision=None, scale=None, null_ok=None)]
        [Column(name='first_name', type_code=1043, display_size=None, internal_size=255, precision=None, scale=None, null_ok=None)]
        [Column(name='last_name', type_code=1043, display_size=None, internal_size=255, precision=None, scale=None, null_ok=None)]
        """
        statement = 'SELECT * FROM "{}"."{}";'.format(schema, table)
        return self.describe_data(statement)


    def delete_all_rows_from_table(self, table, schema='public'):
        """
        Delete all the rows within a given table.

        For example, given we have a table `person` in a database

        When you do the following:
        | Delete All Rows From Table | person |

        If all the rows can be successfully deleted, then you will get:
        | Delete All Rows From Table | person | # PASS |
        If the table doesn't exist or all the data can't be deleted, then you
        will get:
        | Delete All Rows From Table | first_name | # FAIL |
        """
        cur = None
        selectStatement = ('DELETE FROM from "{}"."{}";' % schema, table)
        try:
            cur = self._dbconnection.cursor()
            result = self.__execute_sql(cur, selectStatement)
            if result is not None:
                return result.fetchall()
            self._dbconnection.commit()
        finally :
            if cur :
                self._dbconnection.rollback()

    def execute_sql_script(self, sqlScriptFileName):
        """
        Executes the content of the `sqlScriptFileName` as SQL commands.
        Useful for setting the database to a known state before running
        your tests, or clearing out your test data after running each a
        test.

        Sample usage :
        | Execute Sql Script | ${EXECDIR}${/}resources${/}DDL-setup.sql |
        | Execute Sql Script | ${EXECDIR}${/}resources${/}DML-setup.sql |
        | #interesting stuff here |
        | Execute Sql Script | ${EXECDIR}${/}resources${/}DML-teardown.sql |
        | Execute Sql Script | ${EXECDIR}${/}resources${/}DDL-teardown.sql |

        SQL commands are expected to be delimited by a semi-colon (';').

        For example:
        delete from person_employee_table;
        delete from person_table;
        delete from employee_table;

        Also, the last SQL command can optionally omit its trailing semi-colon.

        For example:
        delete from person_employee_table;
        delete from person_table;
        delete from employee_table

        Given this, that means you can create spread your SQL commands in several
        lines.

        For example:
        delete
          from person_employee_table;
        delete
          from person_table;
        delete
          from employee_table

        However, lines that starts with a number sign (`#`) are treated as a
        commented line. Thus, none of the contents of that line will be executed.

        For example:
        # Delete the bridging table first...
        delete
          from person_employee_table;
          # ...and then the bridged tables.
        delete
          from person_table;
        delete
          from employee_table
        """
        sqlScriptFile = open(sqlScriptFileName)

        cur = None
        try:
            cur = self._dbconnection.cursor()
            sqlStatement = ''
            for line in sqlScriptFile:
                line = line.strip()
                if line.startswith('#'):
                    continue
                elif line.startswith('--'):
                    continue

                sqlFragments = line.split(';')
                if len(sqlFragments) == 1:
                    sqlStatement += line + ' '
                else:
                    for sqlFragment in sqlFragments:
                        sqlFragment = sqlFragment.strip()
                        if len(sqlFragment) == 0:
                            continue

                        sqlStatement += sqlFragment + ' '

                        self.__execute_sql(cur, sqlStatement)
                        sqlStatement = ''

            sqlStatement = sqlStatement.strip()
            if len(sqlStatement) != 0:
                self.__execute_sql(cur, sqlStatement)

            self._dbconnection.commit()
        finally:
            if cur :
                self._dbconnection.rollback()

    def execute_sql_string(self, sqlString):
        """
        Executes the sqlString as SQL commands.
        Useful to pass arguments to your sql.

        SQL commands are expected to be delimited by a semi-colon (';').

        For example:
        | Execute Sql String | delete from person_employee_table; delete from person_table |

        For example with an argument:
        | Execute Sql String | select from person where first_name = ${FIRSTNAME} |
        """
        try:
            cur = self._dbconnection.cursor()
            self.__execute_sql(cur, sqlString)
            self._dbconnection.commit()
        finally:
            if cur:
                self._dbconnection.rollback()

    def __execute_sql(self, cur, sqlStatement):
        logger.debug("Executing : %s" % sqlStatement)
        return cur.execute(sqlStatement)

    def _get_single_result(self, selectStatement):
        return self.query(selectStatement)[0][0]

    def _value_to_text(self, value):
        """
        returns the value of a keyword argument as a SQL text type
        """
        return "'{}'::text".format(value)

