from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from time import sleep, time
import gzip

from tempfile import TemporaryDirectory
import os
from random import randint, choices, seed
from string import ascii_letters

# how much work to generate
scale = 18

# how much parallelism
ratio = 4

# how long can each step be?  (100 ~ 10 sec)
max_hardness = 30

# how many different workloads to randomly assign?
variation = 10

# determines the outcome of pseudorandom choices like whether to sleep or work, and for how long
shape_seed = 2


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

    def __init__(self, n):
        """
        Deterministically come up with some steps to take
        """

        seed(n)
        self.steps = []

        # up to ten steps
        step_numbers = range(randint(1, 10))
        for _ in step_numbers:

            # vary work intensity
            how_hard = randint(0, 20)

            which_type = randint(0, 1)

            # half sleeps, half works
            if which_type == 0:
                self.steps.append((Directive.sleep_step, how_hard))
            else:
                self.steps.append((Directive.work_step, how_hard))

    def walk_steps(self):
        for do_this, like_this in self.steps:
            before = time()
            do_this(like_this)
            duration = time() - before
            print("duration", duration)


@task
def report_params(seed):
    print("scale", scale)
    print("ratio", ratio)
    print("max_hardness", 20)
    print("variation", 15)
    print("seed", seed)


def busy_worker(name):
    @task(task_id=name)
    def busy_work(n):
        work = Directive(n)
        work.walk_steps()

    return busy_work


@dag(
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    default_args={"owner": "airflow"},
    catchup=False,
)
def scheduler_stress():

    # comment this line out for deterministic load shaping
    shape_seed = randint(1, 1000)

    # report the seed for this execution
    start = report_params(shape_seed)
    seed(shape_seed)

    for lane_num in range(1, (scale * ratio) + 1):

        lane = []

        for worker_num in range(1, int(scale / ratio) + 1):
            worker_seed = randint(1, variation)
            worker = busy_worker(f"lane{lane_num}_worker{worker_num}")
            lane.append(worker(worker_seed))

        started = False
        for this_task, next_task in zip(lane, lane[1:]):
            if not started:
                started = True
                start >> this_task
            this_task >> next_task


the_dag = scheduler_stress()
