import itertools
import sys
import threading
import time

class Spinner:
    def __init__(self, message=""):
        self.spinner = itertools.cycle(['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'])
        self.message = message
        self.running = False
        self.thread = None

    def update_message(self, message):
        self.message = message

    def spin(self):
        while self.running:
            sys.stdout.write(f"\r{next(self.spinner)} {self.message} ")
            sys.stdout.flush()
            time.sleep(0.1)
            sys.stdout.write("\b" * (len(self.message) + 3))

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.spin)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        sys.stdout.write(f"\r✅ {self.message}\n")
        sys.stdout.flush()