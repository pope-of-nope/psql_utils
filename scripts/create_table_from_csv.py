import sys
import os
import csv
from typing import List, Dict, Tuple, Set


has_header = True
delimiter = ","
quotechar = "\""
newline = "\n"


FILE_ARGUMENT = sys.argv[1]
FILE_ARGUMENT = os.path.normpath(os.path.abspath(FILE_ARGUMENT))
assert os.path.isfile(FILE_ARGUMENT)


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
                        print("\tvalue '%s' identified column '%s' as nullable" % (row_value, column_names[column_idx]))
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
    ddl = """
    CREATE TABLE x.y ({columns});
    COPY x.y FROM '{filepath}' WITH CSV {header} NULL AS '\\N';
            """.format(columns=column_expressions, filepath=filepath, header='HEADER' if has_header else '')
    print(ddl)
    filename = os.path.basename(filepath)
    sql_filename = filename + ".sql"
    sql_filepath = os.path.join(os.path.dirname(filepath), sql_filename)
    with open(sql_filepath, 'w') as f:
        f.write(ddl + "\n")


if __name__ == '__main__':
    run()
