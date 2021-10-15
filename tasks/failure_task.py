import luigi
from luigi import Task


class FailureTask(Task):
    def run(self):
        1/0


if __name__ == '__main__':
    luigi.build([FailureTask()])

