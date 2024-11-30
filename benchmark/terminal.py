import threading
import queue


class Terminal:
    def __init__(self):
        self.trans_end = 0
        self.finish = False
        self.record = None
        self.ttype = None

    def set_end(self, trans_end):
        self.trans_end = trans_end

    def set_finish(self):
        self.finish = True

    def is_finish(self):
        return self.finish

    def generate(self):
        raise NotImplementedError()

    def finish_time(self):
        raise NotImplementedError()


class TerminalManager:
    def __init__(self, config, scheduler, generator, terminal_type, *term_args):
        self.queue = queue.Queue()
        self.lock = threading.Condition()
        self.scheduler = scheduler
        self.monkeys = []
        # self.terminals = [Terminal]
        self.counter = 0
        self.num_transactions = config.num_transactions
        self.num_monkeys = config.num_monkeys
        self.num_terminals = config.num_terminals

        # Create terminals
        for i in range(config.num_terminals):
            if len(term_args) > 0:
                terminal = terminal_type(generator, *term_args[i])
            else:
                terminal = terminal_type(generator)
            # self.terminals.append(terminal)
            self.queue.put(terminal)

    def start(self):
        # Create and start monkeys
        for _ in range(self.num_monkeys):
            monkey = Monkey(self)
            self.monkeys.append(monkey)
            monkey.start()

    def join(self):
        for monkey in self.monkeys:
            monkey.join()

    def queue_append(self, item):
        with self.lock:
            self.queue.put(item)
            self.lock.notify()


class Monkey(threading.Thread):
    def __init__(self, term_manager: TerminalManager):
        super().__init__()
        self.term_manager = term_manager
        self.lock = term_manager.lock
        self.queue = term_manager.queue
        self.scheduler = term_manager.scheduler

    def run(self):
        while True:
            with self.lock:
                if self.term_manager.counter % 1000 == 0:
                    print(
                        f"Step:{self.term_manager.counter}/{self.term_manager.num_transactions}")
                while self.queue.empty():
                    self.lock.wait()
                if self.term_manager.counter > self.term_manager.num_transactions:
                    self.lock.notify()
                    break
                self.term_manager.counter += 1
                if self.term_manager.counter == self.term_manager.num_transactions+1:
                    terminal = self.queue.get()
                    self.queue.put(terminal)
                    terminal.set_finish()
                    trans_due = terminal.finish_time()
                    self.scheduler.at(trans_due, terminal)
                    self.lock.notify()
                    break
                else:
                    terminal = self.queue.get()
                if not self.queue.empty():
                    self.lock.notify()

            trans_due = terminal.generate()
            self.scheduler.at(trans_due, terminal)
