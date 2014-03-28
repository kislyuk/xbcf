import time

@contextmanager
def timer(label):
    started = time.clock()
    try:
        yield
    finally:
        elapsed = time.clock() - started
        print("{label}: {elapsed}".format(**locals())
