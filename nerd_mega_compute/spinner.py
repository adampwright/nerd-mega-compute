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
        self.last_message_length = 0

    def update_message(self, message):
        # Truncate long messages to prevent UI issues
        max_length = 50  # Keep messages reasonably short
        if len(message) > max_length:
            self.message = message[:max_length-3] + "..."
        else:
            self.message = message
        # Track the longest message for proper clearing
        self.last_message_length = max(self.last_message_length, len(self.message) + 2)

    def spin(self):
        while self.running:
            spinner_char = next(self.spinner)
            # Clear the current line content
            sys.stdout.write('\r')
            # Write the new spinner and message (no newlines)
            sys.stdout.write(f'{spinner_char} {self.message}')
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
        # Clear the current line
        sys.stdout.write('\r')
        # Write final message with a checkmark (and newline)
        sys.stdout.write(f'✅ {self.message}\n')
        sys.stdout.flush()