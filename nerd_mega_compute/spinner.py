import sys
import threading
import time
import itertools

class Spinner:
    """
    A simple spinner to indicate a process is running.
    """
    def __init__(self, message="Processing..."):
        self.message = message
        self.running = False
        self.spinner_thread = None
        self.spinner = itertools.cycle(['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷'])

    def spin(self):
        """Run the spinning animation."""
        while self.running:
            sys.stdout.write(f"\r{next(self.spinner)} {self.message}")
            sys.stdout.flush()
            time.sleep(0.1)
            sys.stdout.write('\b')

    def start(self):
        """Start the spinner animation in a separate thread."""
        self.running = True
        self.spinner_thread = threading.Thread(target=self.spin)
        self.spinner_thread.daemon = True
        self.spinner_thread.start()

    def stop(self):
        """Stop the spinner animation."""
        self.running = False
        if self.spinner_thread and self.spinner_thread.is_alive():
            self.spinner_thread.join(0.1)
        # Clear the spinner line
        sys.stdout.write(f"\r{' ' * (len(self.message) + 2)}\r")
        sys.stdout.flush()

    def update_message(self, message):
        """Update the spinner message."""
        # Clear the current line
        sys.stdout.write(f"\r{' ' * (len(self.message) + 2)}\r")
        sys.stdout.flush()
        self.message = message