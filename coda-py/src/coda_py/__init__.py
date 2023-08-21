import coda_queue as q


def run():
    url = "localhost:2233"
    supervisor = q.Supervisor(url)

    worker = q.Worker()
    worker.run_with_supervisor(supervisor)


if __name__ == '__main__':
    run()
