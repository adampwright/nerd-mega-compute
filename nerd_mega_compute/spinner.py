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
            # Clear the line before writing the new spinner and message
            sys.stdout.write("\r" + " " * (len(self.message) + 10) + "\r")
            sys.stdout.write(f"{next(self.spinner)} {self.message} ")
            sys.stdout.flush()
            time.sleep(0.1)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.spin)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        # Clear the line and write the final message with a checkmark
        sys.stdout.write("\r" + " " * (len(self.message) + 10) + "\r")
        sys.stdout.write(f"✅ {self.message}\n")
        sys.stdout.flush()