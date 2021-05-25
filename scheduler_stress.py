from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from time import sleep, time
from datetime import datetime, timedelta
import gzip
from math import ceil, floor

from tempfile import TemporaryDirectory
import os
from random import randint, choices, seed
from string import ascii_letters

# how much work to generate
scale = 3

# how much parallelism
ratio = 4

# how long can each step be?  (100 ~ 10 sec)
max_hardness = 30

# how many different workloads to randomly assign?
variation_types = 15
variation_count = 3


# determines the outcome of pseudorandom choices like whether to sleep or work, and for how long
# hard code a value here for (more or less) deterministic timing
shape_seed = randint(1, 1000)


class Directive:
    def sleep_step(n):
        """
        sleep for an amount of time determined by n
        n == 100 --> 10 seconds
        """

        print(f"sleeping {n}")
        sleep(n ** 2 * 0.01)
        print("    done")

    def work_step(n):
        """
        work for an amount of time determined by n
        n == 100 --> 10 seconds, more or less
        """

        print(f"working {n}")
        with TemporaryDirectory() as dir:
            fname = os.path.join(dir, "rand")
            with open(fname, "wb") as f:
                bits = "".join(choices(ascii_letters, k=(8000 * n ** 2))).encode(
                    "utf-8"
                )
                compressed = gzip.compress(bits, compresslevel=3)
                f.write(compressed)
            with gzip.open(fname) as f:
                f.read()
        print("    done")

    def sleep_until_10_step(_):
        """
        sleep until the next 10 minute mark
        """
        duration = (-datetime.now().second) % 10
        target = datetime.now() + timedelta(seconds=duration)
        print(f"sleeping until {target.strftime(r'%H:%M:%S')}")
        sleep(duration)
        print("done")

    def __init__(self, n):
        """
        Deterministically come up with some steps to take
        """

        seed(n)
        self.steps = []

        which_type = randint(1, 4)

        if which_type == 1:
            step_numbers = range(randint(1, variation_count))
            for _ in step_numbers:
                how_hard = randint(0, max_hardness)
                if randint(0, 1) == 0:
                    self.steps.append((Directive.sleep_step, how_hard))
                else:
                    self.steps.append((Directive.work_step, how_hard))
        else:
            self.steps = [(Directive.sleep_until_10_step, None)]

    def walk_steps(self):
        for do_this, like_this in self.steps:
            do_this(like_this)


@task
def report_params(seed):
    print("scale", scale)
    print("ratio", ratio)
    print("max_hardness", max_hardness)
    print("variation_types", variation_types)
    print("variation_count", variation_count)
    print("seed", seed)


def busy_worker(name):
    @task(task_id=name)
    def busy_work(n):
        before = time()
        work = Directive(n)
        work.walk_steps()
        duration = time() - before
        print("duration:", duration)

    return busy_work


@dag(
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    default_args={"owner": "airflow"},
    catchup=False,
)
def scheduler_stress():

    # report the seed for this execution
    start = report_params(shape_seed)
    seed(shape_seed)

    for lane_num in range(1, max(ceil(scale * ratio), 2)):

        lane = []

        for worker_num in range(1, max(floor(scale / ratio), 2)):
            worker_seed = randint(1, variation_types)
            worker = busy_worker(f"lane{lane_num}_worker{worker_num}")
            lane.append(worker(worker_seed))

        started = False
        for this_task, next_task in zip(lane, lane[1:]):
            if not started:
                started = True
                start >> this_task
            this_task >> next_task


the_dag = scheduler_stress()
