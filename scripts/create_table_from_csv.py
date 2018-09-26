import re
import sys
import os
import csv
from typing import List, Dict, Tuple, Set, Generator
from collections import Counter
import math


has_header = True
delimiter = ","
quotechar = "\""
newline = "\n"


FILE_ARGUMENT = sys.argv[1]
FILE_ARGUMENT = os.path.normpath(os.path.abspath(FILE_ARGUMENT))
print(FILE_ARGUMENT)
assert os.path.isfile(FILE_ARGUMENT)

STAGING_SCHEMA_NAME = sys.argv[2]


class ColumnValues(object):
    null_values = ["\\N"]
    invalid_values = [('""', "Postgres /COPY can't process empty string \"\". Have you cleaned the file yet?")]

    def __init__(self):
        # type: ()->None
        self.__raw_values = list()  # list of strings
        self.nullable = False  # by default.
        self.__possible_types = [str, int, float]
        self.python_type = None  # by default
        self.__value_counts = Counter(self.__raw_values)
        # self.__max_length = 0

    def add(self, value):
        # type: (str)->None
        # self.__max_length = max(self.__max_length, len(value))
        self.__raw_values.append(value)

    @property
    def sql_type(self):
        if self.python_type == int:
            return "INTEGER"
        elif self.python_type == float:
            return "NUMERIC"
        elif self.python_type == str:
            # postgres TEXT is more useful than you'd expect:
            # https://www.depesz.com/2010/03/02/charx-vs-varcharx-vs-varchar-vs-text/
            return "TEXT"

    def check_for_invalid_values(self):
        invalid_values = [iv for iv, err in self.invalid_values]
        found = [v for v in self.__raw_values if v in invalid_values]
        if any(found):
            found = list(set(found))
            for f in found:
                err = [err for iv, err in self.invalid_values if iv == f][0]
                print(err)
            raise Exception("Invalid values found in file '%s'!" % FILE_ARGUMENT)

    def infer_types(self, verbose=False):
        # the following types are supported: str (TEXT), int (INTEGER), float (NUMERIC)
        # possible_types = [str, int, float]

        def force(value, rule, _type):
            if verbose:
                print("Rule '%s' forced type '%s' on value '%s'" % (rule, _type, value))
            self.__possible_types = [_type]

        def remove(value, rule, _type):
            if _type in self.__possible_types:
                if verbose:
                    print("Rule '%s' eliminated type '%s' for value '%s'" % (rule, _type, value))
                self.__possible_types.remove(_type)

        def eliminate_types(raw_value):
            if re.search('[a-zA-Z]', raw_value):
                force(raw_value, "CONTAINS_ALPHA", str)
                return
            if raw_value.count(".") > 1:
                force(raw_value, "MULTI_DECIMAL", str)
                return
            elif raw_value.count(".") == 1:
                remove(raw_value, "SINGLE_DECIMAL", int)
            elif raw_value.count(".") == 0:
                if raw_value.startswith("0") and raw_value != "0":
                    remove(raw_value, "LEADING_ZERO", int)

        # loop over values.
        for v in [str(v) for v in self.__raw_values]:
            if len(self.__possible_types) == 1:
                # there can be only one
                break

            if v in self.null_values:
                self.nullable = True
                continue  # null values don't tell you about types.

            eliminate_types(v)

        # finally, pick the strictest remaining possible type.
        def pick_strictest_type():
            if len(self.__possible_types) == 1:
                return self.__possible_types[0]
            else:
                if int in self.__possible_types:
                    return int
                elif float in self.__possible_types:
                    return float
                elif str in self.__possible_types:
                    return str
                else:
                    raise ValueError()

        self.python_type = pick_strictest_type()
        self.__value_counts = Counter(self.__raw_values)

    def get_summary(self):
        num_values_total = len(self.__raw_values)
        num_values_unique = len(list(set(self.__raw_values)))
        value_counts = Counter(self.__raw_values)
        return num_values_total, num_values_unique, value_counts

    @property
    def entropy(self):
        unique_values = list(self.__value_counts.keys())
        num_observations = len(self.__raw_values)
        P = {value: float(count)/float(num_observations) for value, count in self.__value_counts.items()}
        assert abs(sum(P.values()) - 1.0) < 0.001, sum(P.values())
        I = {value: -math.log(P[value], math.e) for value in unique_values}
        H = sum([P[value]*I[value] for value in unique_values])
        return H

    @property
    def is_possible_key_column(self):
        if self.python_type in [float]:
            return False
        elif self.python_type in [int, str]:
            H = self.entropy
            H0 = self.entropy_if_uniform
            if abs(H - H0) > 0.00001:
                return False  # not uniform enough.
            else:
                return True

    @property
    def entropy_if_uniform(self):
        """ the entropy expected if this column's unique values were uniformly distributed. """
        unique_values = list(self.__value_counts.keys())
        num_observations = len(self.__raw_values)
        expected_counts_if_uniform = float(num_observations) / float(len(unique_values))
        P = {value: expected_counts_if_uniform / float(num_observations) for value, count in
             self.__value_counts.items()}
        assert abs(sum(P.values()) - 1.0) < 0.001, sum(P.values())
        I = {value: -math.log(P[value], math.e) for value in unique_values}
        H = sum([P[value] * I[value] for value in unique_values])
        return H

    @property
    def max_entropy(self):
        num_observations = len(self.__raw_values)
        P = 1.0/float(num_observations)
        I = -math.log(P, math.e)
        H = P*I*num_observations
        return H


class Column(object):
    def __init__(self, idx, name):
        # type: (int, str)->None
        self.idx = idx
        self.name = name
        self.values = ColumnValues()

    def print_summary(self):
        num_values_total, num_values_unique, value_counts = self.values.get_summary()
        print("\nColumn #%d - %s" % (self.idx, self.name))
        print("\t\tType: %s" % (self.values.python_type))
        print("\t\tnum_values: %s total (%s unique)" % (num_values_total, num_values_unique))
        print("\t\tentropy: %s (expected if uniform: %s)" % (self.values.entropy, self.values.entropy_if_uniform))

    @property
    def column_creation_expression(self):
        return '"{name}" {type} {nullability}'.format(
            name=self.name,
            type=self.values.sql_type,
            nullability="NULL" if self.values.nullable else "NOT NULL"
        )


class ColumnCollection(object):
    def __init__(self):
        self.items = list()

    def add(self, column):
        # type: (Column)->None
        self.items.append(column)

    def __iter__(self):
        # type: ()->Generator[Column]
        for item in self.items:
            yield item

    def getByIdx(self, idx):
        # type: (int)->Column
        return [item for item in self.items if item.idx == idx][0]

    def getByName(self, name):
        # type: (str)->Column
        return [item for item in self.items if item.name == name][0]


class Table(object):
    def __init__(self, schema, name):
        # type: (str, str)->None
        self.schema = schema
        self.name = name
        self.columns = ColumnCollection()
        self.rows = list()

    def sample(self, sample_size, verbose=False):
        # type: (int)->None
        filepath = FILE_ARGUMENT
        # header = has_header
        open_kwargs = {"encoding": "utf8"}
        reader_kwargs = {"delimiter": delimiter, "quotechar": quotechar}

        def get_column_names():
            with open(filepath, 'r', **open_kwargs) as f:
                reader = csv.reader(f, **reader_kwargs)
                first_row = next(reader)
                if has_header:
                    return list([c for c in first_row])
                else:
                    return list(["c_%d" % column for column in first_row])

        column_names = get_column_names()
        for idx, name in enumerate(column_names):
            self.columns.add(Column(idx, name))

        def sample_values():
            n = 0
            with open(filepath, 'r', **open_kwargs) as f:
                reader = csv.reader(f, **reader_kwargs)
                if has_header:
                    header_row = next(reader)
                for data_row in reader:
                    self.rows.append(data_row)
                    for idx, value in enumerate(data_row):
                        self.columns.getByIdx(idx).values.add(value)
                    n += 1
                    if n > sample_size:
                        break

        # sample rows.
        sample_values()

        # infer types.
        for column in self.columns:
            column.values.infer_types(verbose=verbose)

        if verbose:
            for column in self.columns:
                column.print_summary()

    def detect_primary_keys(self):
        """ assumes the primary key will always be made up by the left-most columns. """
        print("\n\n","###" * 30, "Script will now attempt to detect the table's primary key column(s)...")
        possible_key_columns = list()
        for column in self.columns:
            if column.values.is_possible_key_column:
                possible_key_columns.append(column)

        # print("\nThe following %d columns qualify as potential key columns:" % len(possible_key_columns))
        # for column in possible_key_columns:
        #     column.print_summary()

        def check_candidate_key(num_columns):
            # type: (int)->Column
            columns = list([self.columns.getByIdx(idx) for idx in range(num_columns)])
            candidate_key = Column(-1, "candidate_key")
            for row in self.rows:
                value = ",".join(['"%s"' % row[idx] for idx in range(num_columns)])
                candidate_key.values.add(value)
            candidate_key.values.infer_types()
            table_entropy = candidate_key.values.max_entropy
            key_entropy = candidate_key.values.entropy
            if abs(table_entropy - key_entropy) < 0.001:
                print("Entropy analysis suggested the following primary key: ")
                for idx in range(num_columns):
                    column = self.columns.getByIdx(idx)
                    column.print_summary()
                return candidate_key

        def get_primary_key_length():
            for nc in range(len(self.columns.items)):
                c = self.columns.getByIdx(nc)
                if c.values.entropy == c.values.max_entropy:
                    return None
                found = check_candidate_key(nc)
                if found is not None:
                    return nc

        key_length = get_primary_key_length()
        for idx in range(key_length):
            c = self.columns.getByIdx(idx)
            if c.values.python_type != str:
                print("Primary key column %s will be forcibly cast to string" % c.name)
                c.values.python_type = str

        print("All non-primary key columns will be forcibly made nullable.")
        for idx in range(key_length, len(list(self.columns))):
            c = self.columns.getByIdx(idx)
            c.values.nullable = True


class SQLGrammar(object):
    def __init__(self, table):
        # type: (Table)->None
        self.table = table

    def make_drop_table_statement(self):
        return "DROP TABLE IF EXISTS {schema}.\"{table}\";".format(schema=self.table.schema, table=self.table.name)

    def make_create_table_statement(self):
        return "CREATE TABLE {schema}.\"{table}\" ({columns});".format(
            schema=self.table.schema,
            table=self.table.name,
            columns=", ".join([c.column_creation_expression for c in self.table.columns])
        )

    def copy_statement(self):
        return "COPY {schema}.\"{table}\" FROM '{filepath}' WITH CSV {header} NULL AS '\\N';".format(
            schema=self.table.schema,
            table=self.table.name,
            filepath=FILE_ARGUMENT,
            header=" HEADER " if has_header else " ",
        )

    def write_ddl_statements_to_file(self):
        filepath = FILE_ARGUMENT
        drop = self.make_drop_table_statement()
        create = self.make_create_table_statement()
        copy = self.copy_statement()
        sql = drop + "\n" + create + "\n" + copy + "\n"

        filename = os.path.basename(filepath)
        sql_filename = filename + ".sql"
        sql_filepath = os.path.join(os.path.dirname(filepath), sql_filename)
        with open(sql_filepath, 'w') as f:
            f.write(sql)
        pass


def run_v2():
    table_name = str(os.path.basename(FILE_ARGUMENT).split(".")[0])
    table = Table(schema=STAGING_SCHEMA_NAME, name=table_name)
    table.sample(sample_size=10000, verbose=False)
    table.detect_primary_keys()
    sql = SQLGrammar(table)
    sql.write_ddl_statements_to_file()


def run():
    DONT_CHECK_NULLS = True
    filepath = FILE_ARGUMENT

    open_kwargs = {"encoding": "utf8"}

    print("Previewing file: ")
    with open(filepath, 'r', **open_kwargs) as f:
        i = 0
        for line in f:
            i += 1
            print(line)
            if i > 3:
                break

    open_kwargs["newline"] = newline
    reader_kwargs = {"delimiter": delimiter, "quotechar": quotechar}

    def get_column_names():
        with open(filepath, 'r', **open_kwargs) as f:
            reader = csv.reader(f, **reader_kwargs)
            first_row = next(reader)
            if has_header:
                return list([c for c in first_row])
            else:
                return list(["c_%d" % column for column in first_row])

    column_names = get_column_names()
    columns_dict = {idx: name for idx, name in enumerate(column_names)}

    print("Identified the following column names: ", column_names)

    def determine_column_types(sample_size=1000):
        # type: (int)->Tuple[Dict[int, type], Set[int]]
        with open(filepath, 'r', **open_kwargs) as f:
            reader = csv.reader(f, **reader_kwargs)
            null_values = [r"\N", "", "%s%s" % (quotechar, quotechar)]

            if has_header:
                discard = next(reader)

            sample = []
            for row in reader:
                if len(sample) < sample_size:
                    sample.append(row)
                else:
                    break

            possible_types = {idx: [int, float, str] for idx in columns_dict.keys()}
            undetermined_columns = [idx for idx in columns_dict.keys()]
            nullable_columns = set()

            def eliminate_possible_types(column_idx, row_value):
                # type: (int, str)->None
                if row_value in null_values:
                    return

                decimal_count = row_value.count(".")
                if decimal_count == 1:
                    if int in possible_types[column_idx]:
                        print("\tvalue '%s' eliminated type '%s' for column '%s'" % (
                        row_value, int, column_names[column_idx]))
                        possible_types[column_idx].remove(int)
                elif decimal_count > 1:
                    if int in possible_types[column_idx]:
                        print("\tvalue '%s' eliminated type '%s' for column '%s'" % (
                        row_value, int, column_names[column_idx]))
                        possible_types[column_idx].remove(int)
                    if float in possible_types[column_idx]:
                        print("\tvalue '%s' eliminated type '%s' for column '%s'" % (
                        row_value, float, column_names[column_idx]))
                        possible_types[column_idx].remove(float)
                if not row_value.replace(".", "").isnumeric():
                    if int in possible_types[column_idx]:
                        print("\tvalue '%s' eliminated type '%s' for column '%s'" % (
                        row_value, int, column_names[column_idx]))
                        possible_types[column_idx].remove(int)
                    if float in possible_types[column_idx]:
                        print("\tvalue '%s' eliminated type '%s' for column '%s'" % (
                        row_value, float, column_names[column_idx]))
                        possible_types[column_idx].remove(float)

                if len(possible_types[column_idx]) == 1:
                    if column_idx in undetermined_columns:
                        print("Finalized type '%s' for column '%s'" % (
                        possible_types[column_idx][0], column_names[column_idx]))
                        undetermined_columns.remove(column_idx)

            def identify_nullable_columns(column_idx, row_value):
                if row_value in null_values:
                    if column_idx not in nullable_columns:
                        # print("\tvalue '%s' identified column '%s' as nullable" % (row_value, column_names[column_idx]))
                        nullable_columns.add(column_idx)

            for row in sample:
                for column_idx in undetermined_columns:
                    eliminate_possible_types(column_idx, row[column_idx])
                for idx, value in enumerate(row):
                    identify_nullable_columns(idx, value)

            def pick_strictest_type(column_idx):
                possible = possible_types[column_idx]
                if int in possible:
                    return int
                elif float in possible:
                    return float
                elif str in possible:
                    return str
                elif len(possible) == 1:
                    return possible[0]
                else:
                    raise ValueError(possible)

            determined_types = {column: pick_strictest_type(column) for column in columns_dict.keys()}
            return determined_types, nullable_columns

    column_types, nullable_columns = determine_column_types(sample_size=100000)
    print("Finished determining column types.")

    def make_column_expression(idx):
        # type: (int)->str
        column_name = columns_dict[idx]
        if column_name.startswith(quotechar) and column_name.endswith(quotechar):
            pass
        else:
            column_name = "{qc}{cn}{qc}".format(qc=quotechar, cn=column_name)
        is_nullable = idx in nullable_columns
        python_type = column_types[idx]
        python_to_pg_type = {int: 'INTEGER', float: 'NUMERIC', str: 'TEXT'}
        pg_type = python_to_pg_type[python_type]
        nullability = "NULL" if is_nullable else "NOT NULL"
        expression = "{column_name} {pg_type} {nullability}".format(
            column_name=column_name, nullability=nullability, pg_type=pg_type)
        return expression

    column_expressions = ", ".join([make_column_expression(idx) for idx in range(len(column_names))])

    filename = os.path.basename(filepath)
    TABLE_NAME = filename.split(".")[0]
    sql_filename = filename + ".sql"
    sql_filepath = os.path.join(os.path.dirname(filepath), sql_filename)

    ddl = """CREATE TABLE {x}.{y} ({columns}); COPY {x}.{y} FROM '{filepath}' WITH CSV {header} NULL AS '\\N';""".format(
        columns=column_expressions, filepath=filepath, header='HEADER' if has_header else '',
        x=STAGING_SCHEMA_NAME, y=TABLE_NAME
    )
    print(ddl)

    filename = os.path.basename(filepath)
    sql_filename = filename + ".sql"
    sql_filepath = os.path.join(os.path.dirname(filepath), sql_filename)
    with open(sql_filepath, 'w') as f:
        f.write(ddl + "\n")


if __name__ == '__main__':
    run_v2()
    # run()
