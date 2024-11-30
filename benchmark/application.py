import threading
import queue
import time
from utils import clear_path, dump_transaction


class Application(threading.Thread):
    def __init__(self, id, conn, queue, lock, terminal_manager, config):
        super().__init__()
        self.id = id
        self.conn = conn
        self.queue = queue
        self.lock = lock
        self.terminal_manager = terminal_manager
        self.config = config

    def do(self, ttype, record):
        return getattr(self, ttype)(record)


    def run(self):
        output_path = self.config.output_path(self.id)
        clear_path(output_path)
        while True:
            with self.lock:
                while self.queue.empty():
                    self.lock.wait()
                terminal = self.queue.get()
                if terminal.finish:
                    self.queue.put(terminal)
                    self.lock.notify()
                    break
                if not self.queue.empty():
                    self.lock.notify()
            transaction = self.do(terminal.ttype, terminal.record)

            dump_transaction(transaction, output_path)
            self.terminal_manager.queue_append(terminal)


class ApplicationManager:
    def __init__(self, config, database, term_manager, app_type, *args):
        # scheduler puts terminal's data into this queue
        self.queue = queue.Queue()
        self.applications = []
        self.database = database
        self.lock = threading.Condition()
        for i in range(config.num_sessions):
            conn = database.connect()
            if len(args) > 0:
                application = app_type(
                    i, conn, self.queue, self.lock, term_manager, *args)
            else:
                application = app_type(
                    i, conn, self.queue, self.lock, term_manager)
            self.applications.append(application)

    def start(self):
        for application in self.applications:
            application.start()

    def join(self):
        for application in self.applications:
            application.join()

    def terminate(self):
        for application in self.applications:
            application.join()

    def queue_append(self, item):
        with self.lock:
            self.queue.put(item)
            self.lock.notify()
