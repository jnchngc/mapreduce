"""Mapreduce worker node implementation."""
import os
import logging
import json
from pathlib import Path
import socket
import time
import threading
import subprocess
import click
from mapreduce import utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


def sanitize_output_dir(message):
    """Take away trailing slashes on an output directory."""
    output_dir = Path(message['output_directory'])
    message['output_directory'] = str(output_dir)
    return message


class Worker:
    """Worker class executes jobs from master node."""

    def __init__(self, master_port, worker_port):
        """Construct worker node."""
        # logging.info("Starting worker:%s", worker_port)
        # logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        # This is a fake message to demonstrate pretty printing with logging
        # logging.debug(
        #     "Worker:%s received\n%s",
        #     worker_port,
        #     json.dumps(message_dict, indent=2),
        # )
        self.pid = os.getpid()
        self.master_port = master_port
        self.worker_port = worker_port
        self.shutdown_received = False
        self.listen_thread = None
        self.heartbeat_thread = None
        self.create_threads()
        self.register_worker()
        self.listen_thread.join()
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()

    def create_threads(self):
        """Create listen thread used in program (heartbeat created later)."""
        self.listen_thread = threading.Thread(target=self.listen)
        self.listen_thread.start()

    def register_worker(self):
        """Register self with the master node."""
        message = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": self.worker_port,
            "worker_pid": self.pid
        }
        utils.send_tcp_message(self.master_port, message)

    def listen(self):
        """Listen on TCP connection for messages from master."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", self.worker_port))
        sock.listen()
        sock.settimeout(1)
        while True:
            try:
                worker_clientsocket = sock.accept()[0]
            except socket.timeout:
                continue
            worker_message_chunks = []
            while True:
                try:
                    data = worker_clientsocket.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                worker_message_chunks.append(data)
            worker_clientsocket.close()
            worker_message_bytes = b''.join(worker_message_chunks)
            message_str = worker_message_bytes.decode("utf-8")
            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            self.handle_request(message_dict)
            if self.shutdown_received:
                break
        sock.close()

    def handle_request(self, message_dict):
        """Respond to the message a worker receives over TCP."""
        if message_dict['message_type'] == 'shutdown':
            self.shutdown_received = True
        elif message_dict['message_type'] == 'register_ack':
            self.create_heartbeat()
        elif message_dict['message_type'] == 'new_worker_job':
            message = sanitize_output_dir(message_dict)
            self.execute_job(message)
        elif message_dict['message_type'] == 'new_sort_job':
            self.execute_sort_job(message_dict)

    def execute_job(self, message_dict):
        """Execute a job according to message information."""
        output_files = []
        for in_file in message_dict['input_files']:
            input_file_obj = open(Path(in_file), 'r')
            file_name = (in_file.split('/'))[-1]
            output_path = f"{message_dict['output_directory']}/{file_name}"
            output_file_obj = open(Path(output_path), 'w')
            subprocess.run(
                args=[message_dict['executable']], stdin=input_file_obj,
                stdout=output_file_obj, check=False
            )
            output_files.append(output_path)
        worker_output = {
            "message_type": "status",
            "output_files": output_files,
            "status": "finished",
            "worker_pid": self.pid
        }
        utils.send_tcp_message(self.master_port, worker_output)

    def execute_sort_job(self, message_dict):
        """Sort input files into one sorted output file."""
        all_lines = []
        for input_file in message_dict['input_files']:
            with open(Path(input_file), 'r') as open_file:
                lines_in_file = open_file.readlines()
            all_lines += lines_in_file
        all_lines.sort()
        sorted_file = Path(message_dict['output_file'])
        with open(sorted_file, 'w') as open_file:
            for line in all_lines:
                open_file.write(f'{line}')
        worker_output = {
            "message_type": "status",
            "output_file": message_dict['output_file'],
            "status": "finished",
            "worker_pid": self.pid
        }
        utils.send_tcp_message(self.master_port, worker_output)

    def create_heartbeat(self):
        """Create a UDP connection to send heartbeats to master."""
        heartbeat_sock = utils.create_socket(is_tcp=False)
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat,
                                                 args=(heartbeat_sock,))
        self.heartbeat_thread.start()

    def send_heartbeat(self, sock):
        """Send a heartbeat message to the master."""
        message_dict = {'message_type': 'heartbeat', 'worker_pid': self.pid}
        sock.connect(("localhost", self.master_port - 1))
        while True:
            if self.shutdown_received:
                break
            message = json.dumps(message_dict)
            sock.sendall(message.encode('utf-8'))
            time.sleep(2)
        sock.close()


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Make worker node from class."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
