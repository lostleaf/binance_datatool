import fire

from bhds.cli import Bhds
from bmac.cli import Bmac


class Task:

    def __init__(self):
        self.bhds = Bhds()
        self.bmac = Bmac()


if __name__ == '__main__':
    fire.Fire(Task)
