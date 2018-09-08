from collections import defaultdict, Counter

from core import Task, Interface, TaskContext, logger, Cancel, TaskResult
from typing import Set, List, Dict, Tuple, Any, Callable
import os
import csv


class TaskSwitch(Task):
    #options: List[Tuple[type(Task), str]] = []
    options = list()

    def on_call(self, *args, **kwargs):
        next_task = self.context.interface.select_prompt("Select a task:", options=self.options)
        return self.context.init_and_call(next_task)


class InputTask(Task):
    def get_prompt(self):
        raise NotImplementedError()

    def sanitize(self, raw_value):
        return raw_value

    def validate(self, value):
        # type: ()->bool
        raise NotImplementedError()

    def on_call(self, *args, **kwargs):
        def attempt():
            # type: ()->object
            prompt = self.get_prompt()
            value = input(prompt)
            valid = self.validate(value)
            if valid:
                return self.sanitize(value)
            else:
                retry = input("Validation error. Try again? y/n: ")
                retry = True if retry.lower() in ['y', 'yes'] else False
                if retry:
                    return attempt()
                else:
                    raise Cancel()
        try:
            value = attempt()
            self.context.done(value)
        except Cancel as e:
            print("Cancelling.")
            self.context.cancel(e)


class Choice(Task):
    @classmethod
    def init(cls, parent, prompt, options):
        # type: (Task, str, List[Tuple[str, Any]])->Callable[Any, TaskResult]
        return parent.context.init(cls, prompt=prompt, options=options)

    @classmethod
    def call(cls, parent, prompt, options):
        # type: (Task, str, List[Tuple[str, Any]])->TaskResult
        return parent.context.init(cls, prompt=prompt, options=options)()

    def __init__(self, context, prompt, options):
        # type: (TaskContext, str, List[Tuple[str, Any]])->None
        super().__init__(context)
        self.prompt = prompt
        self.options = options

    def on_call(self, *args, **kwargs):
        print(self.prompt)
        for idx, (label, value) in enumerate(self.options):
            print("\t[%d.] %s" % (idx, label))
        selected_index = input("Enter a number: ")
        selected_index = int(selected_index)
        selected_value = self.options[selected_index][1]
        self.context.done(selected_value)


class YesOrNo(InputTask):
    @classmethod
    def call(cls, parent, prompt="Enter yes or no: "):
        # type: (Task)->TaskResult
        return parent.context.init_and_call(cls, cls__prompt=prompt)

    def __init__(self, context, prompt):
        # type: (TaskContext, str)->None
        super().__init__(context)
        self.prompt = prompt

    def get_prompt(self):
        return self.prompt

    def sanitize(self, raw_value):
        if raw_value.lower() in ['y', 'yes', '1']:
            return True
        elif raw_value.lower() in ['n', 'no', '0']:
            return False
        else:
            raise ValueError()

    def validate(self, value):
        try:
            temp = self.sanitize(value)
            return True
        except ValueError():
            return False


class GetFilenameTask(InputTask):
    def get_prompt(self):
        return "Enter the filepath: "

    def validate(self, value):
        if os.path.isfile(value):
            if os.path.isabs(value):
                return True
            else:
                abs = os.path.abspath(value)
                if os.path.isfile(abs) and os.path.isabs(abs):
                    return True
                else:
                    logger.error("Could not make absolute filepath from relative path: %s" % value)
                    return False
        if not os.path.exists(value):
            logger.error("Path does not exist: %s" % value)
            return False
        if os.path.isdir(value):
            logger.error("Path is a directory, not a file: %s" % value)
            return False

    def sanitize(self, raw_value):
        return os.path.normpath(os.path.abspath(raw_value))

    # def on_call(self, *args, **kwargs):
    #     filepath = input("Enter the filepath: ")
    #     if os.path.isfile(filepath):
    #         self.context.done(filepath)
    #     if not os.path.exists(filepath):
    #         logger.error("Path does not exist: %s" % filepath)


class CreateTableFromCsvTask(Task):
    def on_call(self, *args, **kwargs):
        result = self.context.init_and_call(GetFilenameTask)
        filepath = result.success
        if filepath is None:
            self.cancel()

        open_kwargs = {"encoding": "utf8"}

        print("Previewing file: ")
        with open(filepath, 'r', **open_kwargs) as f:
            i = 0
            for line in f:
                i += 1
                print(line)
                if i > 3:
                    break

        def get_result(result):
            # type: (TaskResult)->Any
            return result.success

        has_header = get_result(self.context.init(YesOrNo, "Does this file have a header?  ")())
        # delimiter: str = get_result(Choice.call(self, "Select the delimiter: ", [
        #     ("comma", ","),
        #     ("tab", "\t"),
        #     ("space", " "),
        #     ("pipe", "|"),
        # ]))
        # quotechar: str = get_result(Choice.call(self, "Select an escape character: ", [
        #     ("double quotes", "\""),
        #     ("single quotes", "'"),
        # ]))
        # newline: str = get_result(Choice.call(self, "Newline character: ", [
        #     ("*nix style", "\n"),
        #     ("windows style", "\r\n"),
        # ]))
        delimiter = ","
        quotechar = "\""
        newline = "\n"

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
                            print("\tvalue '%s' eliminated type '%s' for column '%s'" % (row_value, int, column_names[column_idx]))
                            possible_types[column_idx].remove(int)
                    elif decimal_count > 1:
                        if int in possible_types[column_idx]:
                            print("\tvalue '%s' eliminated type '%s' for column '%s'" % (row_value, int, column_names[column_idx]))
                            possible_types[column_idx].remove(int)
                        if float in possible_types[column_idx]:
                            print("\tvalue '%s' eliminated type '%s' for column '%s'" % (row_value, float, column_names[column_idx]))
                            possible_types[column_idx].remove(float)
                    if not row_value.replace(".", "").isnumeric():
                        if int in possible_types[column_idx]:
                            print("\tvalue '%s' eliminated type '%s' for column '%s'" % (row_value, int, column_names[column_idx]))
                            possible_types[column_idx].remove(int)
                        if float in possible_types[column_idx]:
                            print("\tvalue '%s' eliminated type '%s' for column '%s'" % (row_value, float, column_names[column_idx]))
                            possible_types[column_idx].remove(float)

                    if len(possible_types[column_idx]) == 1:
                        if column_idx in undetermined_columns:
                            print("Finalized type '%s' for column '%s'" % (possible_types[column_idx][0], column_names[column_idx]))
                            undetermined_columns.remove(column_idx)

                def identify_nullable_columns(column_idx, row_value):
                    if row_value in null_values:
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

        column_types, nullable_columns = determine_column_types(sample_size=1000)
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


class CreateTableTask(TaskSwitch):
    options = [
        (CreateTableFromCsvTask, "From CSV file"),
    ]


class RootTask(TaskSwitch):
    options = [
        (CreateTableTask, "Create a table from a file"),
    ]


if __name__ == '__main__':
    context = TaskContext()
    context.init_and_call(RootTask)
