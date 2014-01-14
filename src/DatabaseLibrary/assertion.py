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
        results, count, __ = self.__run_query(selectStatement)
        if (count > 0):
            raise AssertionError("Expected no results to be returned from '%s'. "
                                 "Actually returned: %s\n (%s results)" %
                                 (selectStatement, results, count))


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

    def table_must_exist(self,tableName):
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
            raise AssertionError("Table '%s' does not exist in the db" % tableName)


