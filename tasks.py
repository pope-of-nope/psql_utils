from core import Task, Interface, TaskContext, logger, Cancel, TaskResult
from typing import Set, List, Dict, Tuple
import os


class TaskSwitch(Task):
    options: List[Tuple[type(Task), str]]

    def on_call(self, *args, **kwargs):
        self.context.interface.select_prompt("Select a task:", options=self.options)


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
        except Cancel:
            print("Cancelling.")
            self.context.cancel()


class YesOrNo(InputTask):
    @classmethod
    def call(cls, parent, prompt="Enter yes or no: "):
        # type: (Task)->TaskResult
        return parent.context.call(cls, cls__prompt=prompt)

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
        context = self.context

        result = self.context.call(GetFilenameTask)
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
                print("\n")
                if i > 3:
                    break
        has_header = YesOrNo.call(self, "Does this file have a header?")


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
    context.call(RootTask)
