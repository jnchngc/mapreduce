"""Mapreduce master node implementation."""
import os
import logging
import json
from pathlib import Path
from collections import OrderedDict, deque
import socket
import threading
import shutil
from datetime import datetime
import heapq
import click
from mapreduce import utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)

# OH QUESTIONS
#
# 1. what happens if there are no workers?


class Master:
    """Master class listens to instructions and executes them."""

    def __init__(self, port):
        """Construct master node."""
        # logging.info("Starting master:%s", port)
        # logging.info("Master:%s PWD %s", port, os.getcwd())
        self.shutdown_received = False
        self.job_count = 0
        self.current_job = {}
        self.job_queue = deque()
        self.stage_info = {
            'current_stage': 'nothing',
            'tasks_completed': 0,
            'total_stage_tasks': 0
        }
        self.task_queue = []
        self.workers = OrderedDict()
        self.__prepare_server(port)

    def __prepare_server(self, port):
        """Create tmp dir and threads used in master execution."""
        path = Path('.')/'tmp'
        path.mkdir(exist_ok=True)
        file_names = path.glob('job-*')
        for name in file_names:
            shutil.rmtree(name)
        listen_thread = threading.Thread(target=self.__listen, args=(port,))
        listen_thread.start()
        heartbeat_thread = threading.Thread(target=self.__heartbeat,
                                            args=(port,))
        heartbeat_thread.start()
        # join all threads to ensure they all break and are destroyed
        listen_thread.join()
        heartbeat_thread.join()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~~~ HEARTBEAT THREAD FUNCTIONS BELOW ~~~
    def __heartbeat(self, port):
        """Wait on worker heartbeat messages via UDP."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port - 1))
        sock.settimeout(1)
        while True:
            if self.shutdown_received:
                break
            try:
                clientsocket = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = clientsocket.decode("utf-8")
            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            if message_dict['message_type'] == 'heartbeat':
                worker_pid = message_dict['worker_pid']
                if worker_pid in self.workers:
                    self.workers[worker_pid]['prev_ping_time'] = datetime.now()
            for worker_info in self.workers.values():
                if (worker_info['status'] != 'dead' and (datetime.now() -
                   worker_info['prev_ping_time']).total_seconds() >= 10):
                    worker_info['status'] = 'dead'
                    self.task_queue.append(worker_info['current_task'])
                    self.work_on_tasks()
        sock.close()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~~~ LISTEN THREAD FUNCTIONS BELOW ~~~
    def __listen(self, port):
        """Wait on incoming messages via TCP."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port))
        sock.listen()
        sock.settimeout(1)
        while True:
            try:
                clientsocket = sock.accept()[0]
            except socket.timeout:
                continue
            message_chunks = []
            while True:
                try:
                    data = clientsocket.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                message_chunks.append(data)
            clientsocket.close()
            message_bytes = b''.join(message_chunks)
            message_str = message_bytes.decode("utf-8")
            try:
                message_dict = json.loads(message_str)
            except json.JSONDecodeError:
                continue
            self.__handle_message(message_dict)
            if self.shutdown_received:
                break
        sock.close()

    def __handle_message(self, message_dict):
        """Handle TCP messages according to their content."""
        # logging.debug(
        #     "\n Recieved \n %s \n\n",
        #     json.dumps(message_dict, indent=2)
        # )
        if message_dict['message_type'] == 'shutdown':
            self.shutdown_received = True
            self.__forward_shutdown(message_dict)
        elif message_dict['message_type'] == 'register':
            self.__register_worker(message_dict)
            self.distribute_work()
        elif message_dict['message_type'] == 'new_master_job':
            self.__schedule_new_job(message_dict)
            self.distribute_work()
        elif (message_dict['message_type'] == 'status' and
              message_dict['status'] == 'finished'):
            self.handle_finish(message_dict)
            self.distribute_work()

    def __forward_shutdown(self, message):
        """Forward shutdown message to all workers."""
        for value in self.workers.values():
            if value['status'] != 'dead':
                utils.send_tcp_message(value['worker_port'], message)

    def __register_worker(self, message):
        """Register a worker with the master."""
        self.workers[message['worker_pid']] = {
            'status': 'ready',
            'worker_port': message['worker_port'],
            'prev_ping_time': datetime.now()
        }
        ack_message = {
            "message_type": "register_ack",
            "worker_host": message['worker_host'],
            "worker_port": message['worker_port'],
            "worker_pid": message['worker_pid']
        }
        utils.send_tcp_message(message['worker_port'], ack_message)

    def __schedule_new_job(self, message_dict):
        """Add job to queue and run it if possible."""
        self.create_job_directory()
        input_dir = Path(message_dict['input_directory'])
        message_dict['input_directory'] = str(input_dir)
        output_dir = Path(message_dict['output_directory'])
        message_dict['output_directory'] = str(output_dir)
        message_dict['id'] = self.job_count
        self.job_count += 1
        self.job_queue.append(message_dict)

    def create_job_directory(self):
        """Create job directories for calculations."""
        path = Path('./tmp')/f'job-{self.job_count}'
        path.mkdir()
        mapper_p = path/'mapper-output'
        mapper_p.mkdir()
        grouper_p = path/'grouper-output'
        grouper_p.mkdir()
        reducer_p = path/'reducer-output'
        reducer_p.mkdir()

    def handle_finish(self, message_dict):
        """Account for completed task in status message."""
        pid = message_dict['worker_pid']
        if pid in self.workers and self.workers[pid]['status'] != 'dead':
            self.stage_info['tasks_completed'] += 1
            self.workers[message_dict['worker_pid']]['status'] = 'ready'

    # execution logic
    def distribute_work(self):
        """Send next task to available workers, start next stage if needed."""
        if self.stage_can_start():
            self.start_next_stage()
        else:
            self.work_on_tasks()

    def stage_can_start(self):
        """Check if a stage can start (starting a job or progressing a job)."""
        return (self.stage_info['tasks_completed'] ==
                self.stage_info['total_stage_tasks'] or
                self.job_can_start())

    def start_next_stage(self):
        """Start next stage in sequence if current stage or job is done."""
        if self.stage_info['current_stage'] == 'nothing':
            if len(self.job_queue) > 0:
                self.stage_info['current_stage'] = 'map'
                self.start_map_stage()
        elif self.stage_info['current_stage'] == 'map':
            self.stage_info['current_stage'] = 'group'
            self.start_group_stage()
        elif self.stage_info['current_stage'] == 'group':
            self.master_grouping()
            self.stage_info['current_stage'] = 'reduce'
            self.start_reduce_stage()
        elif self.stage_info['current_stage'] == 'reduce':
            self.present_output()
            self.current_job = {}
            self.stage_info['current_stage'] = 'nothing'
            self.start_next_stage()

    def work_on_tasks(self):
        """Distribute tasks in task queue to workers."""
        for worker_pid, worker_info in self.workers.items():
            if len(self.task_queue) == 0:
                break
            if worker_info['status'] == 'ready':
                worker_info['status'] = 'busy'
                message = self.create_task_message(worker_pid)
                utils.send_tcp_message(worker_info['worker_port'], message)
                worker_info['current_task'] = self.task_queue[0]
                self.task_queue.pop(0)

    def create_task_message(self, worker_pid):
        """Format message sent to a worker according to the current stage."""
        if self.stage_info['current_stage'] == 'map':
            out_dir = f"tmp/job-{self.current_job['id']}/mapper-output"
            message = {
                "message_type": "new_worker_job",
                "input_files": self.task_queue[0],
                "executable": self.current_job['mapper_executable'],
                "output_directory": out_dir,
                "worker_pid": worker_pid
            }
        elif self.stage_info['current_stage'] == 'group':
            out_dir = f"tmp/job-{self.current_job['id']}/grouper-output"
            message = {
                "message_type": "new_sort_job",
                "input_files": self.task_queue[0],
                "output_directory": out_dir,
                "worker_pid": worker_pid
            }
        elif self.stage_info['current_stage'] == 'reduce':
            out_dir = f"tmp/job-{self.current_job['id']}/reducer-output"
            message = {
                "message_type": "new_worker_job",
                "input_files": self.task_queue[0],
                "executable": self.current_job['reducer_executable'],
                "output_directory": out_dir,
                "worker_pid": worker_pid
            }
        return message
    # end of execution logic

    def job_can_start(self):
        """Check if workers ready and there is a job waiting."""
        workers_ready = False
        for value in self.workers.values():
            if value['status'] == 'busy':
                workers_ready = False
                break
            if value['status'] == 'ready':
                workers_ready = True
        return (not self.current_job and workers_ready and
                len(self.job_queue) > 0)

    # ///////////// MAP STAGE
    def start_map_stage(self):
        """Partitions input and assign mapping tasks to workers."""
        self.current_job = self.job_queue[0]
        self.job_queue.popleft()
        self.stage_info['tasks_completed'] = 0
        self.partition_map_input()
        self.stage_info['total_stage_tasks'] = self.current_job['num_mappers']
        self.send_map_tasks()

    def partition_map_input(self):
        """Partition input into num_mappers groups."""
        input_dirname = self.current_job['input_directory']
        path = Path(input_dirname)
        input_files = os.listdir(path)
        input_files.sort()
        partition_result = [[] for _ in range(self.current_job['num_mappers'])]
        for count, item in enumerate(input_files):
            round_robin = count % self.current_job['num_mappers']
            partition_result[round_robin].append(input_dirname + "/" + item)
        self.task_queue = partition_result

    def send_map_tasks(self):
        """Send files to workers to complete."""
        for pid, worker_info in self.workers.items():
            if len(self.task_queue) == 0:
                break
            if worker_info['status'] == 'ready':
                out_dir = f"tmp/job-{self.current_job['id']}/mapper-output"
                map_job = {
                    "message_type": "new_worker_job",
                    "input_files": self.task_queue[0],
                    "executable": self.current_job['mapper_executable'],
                    "output_directory": out_dir,
                    "worker_pid": pid
                }
                utils.send_tcp_message(worker_info['worker_port'], map_job)
                worker_info['current_task'] = self.task_queue[0]
                self.task_queue.pop(0)
                worker_info['status'] = 'busy'

    # ///////////// GROUP STAGE
    def start_group_stage(self):
        """Execute grouping stage of map reduce."""
        mapped_files = self.retrieve_stage_output('mapper-output')
        self.partition_group_input(mapped_files)
        self.stage_info['tasks_completed'] = 0
        self.stage_info['total_stage_tasks'] = len(self.task_queue)
        self.send_group_tasks()

    def retrieve_stage_output(self, output_folder):
        """Get the files from stage for input to next stage."""
        path = Path(f"tmp/job-{self.current_job['id']}/{output_folder}")
        completed_files = path.glob('*')
        completed_files = [str(path) for path in completed_files]
        completed_files.sort()
        return completed_files

    def partition_group_input(self, mapped_files):
        """Partition mapper output files into num workers groups."""
        num_live_workers = 0
        for worker_info in self.workers.values():
            if worker_info['status'] == 'ready':
                num_live_workers += 1
        groupers_needed = min(num_live_workers, len(mapped_files))
        list_of_tasks = [[] for _ in range(groupers_needed)]
        for count, item in enumerate(mapped_files):
            round_robin_index = count % groupers_needed
            list_of_tasks[round_robin_index].append(item)
        for count, item in enumerate(list_of_tasks):
            out_num = f'0{count + 1}' if count < 9 else f'{count + 1}'
            out_dir = f"tmp/job-{self.current_job['id']}/grouper-output"
            out_path = f'{out_dir}/sorted{out_num}'
            self.task_queue.append({
                'input_files': item,
                'output_path': out_path
            })

    def send_group_tasks(self):
        """Send group jobs out to workers."""
        for pid, worker_info in self.workers.items():
            if len(self.task_queue) == 0:
                break
            if worker_info['status'] == 'ready':
                cur_task = self.task_queue[0]
                group_job = {
                    "message_type": "new_sort_job",
                    "input_files": cur_task['input_files'],
                    "output_file": cur_task['output_path'],
                    "worker_pid": pid
                }
                utils.send_tcp_message(worker_info['worker_port'], group_job)
                worker_info['current_task'] = self.task_queue[0]
                self.task_queue.pop(0)

    def master_grouping(self):
        """Sort worker's grouping output into num_reducers files."""
        open_sorted_files = []
        open_reducer_files = []
        reduce_path = f"tmp/job-{self.current_job['id']}/grouper-output/reduce"
        num_reducers = self.current_job['num_reducers']
        for i in range(1, num_reducers+1):
            file_path = f'{reduce_path}{(str(0) + str(i)) if i < 10 else i}'
            open_reducer_files.append(open(Path(file_path), 'w'))
        output_files = self.retrieve_stage_output('grouper-output')
        for output_file in output_files:
            open_sorted_files.append(open(output_file, 'r'))
        line_num = -1
        current_key = None
        for line in heapq.merge(*open_sorted_files):
            pair = line.split('\t')
            if pair[0] != current_key:
                line_num += 1
                current_key = pair[0]
            index = line_num % num_reducers
            open_reducer_files[index].write(line)
        for open_file in open_sorted_files:
            open_file.close()
        for open_file in open_reducer_files:
            open_file.close()

    # ///////////// REDUCE STAGE
    def start_reduce_stage(self):
        """Carry out reduce stage for mapreduce."""
        self.partition_reduce_input()
        self.stage_info['tasks_completed'] = 0
        self.stage_info['total_stage_tasks'] = self.current_job['num_reducers']
        self.send_reduce_tasks()

    def partition_reduce_input(self):
        """Partition input into num_reducers groups."""
        input_dirname = f"tmp/job-{self.current_job['id']}/grouper-output"
        path = Path(input_dirname)
        glob_files = path.glob('reduce*')
        input_files = []
        for globbed in glob_files:
            input_files.append(str(globbed))
        num_reducers = self.current_job['num_reducers']
        partition_result = [[] for _ in range(num_reducers)]
        for count, item in enumerate(input_files):
            round_robin_index = count % self.current_job['num_reducers']
            partition_result[round_robin_index].append(item)
        self.task_queue = partition_result

    def send_reduce_tasks(self):
        """Send files to workers to complete."""
        for pid, worker_info in self.workers.items():
            if len(self.task_queue) == 0:
                break
            if worker_info['status'] == 'ready':
                out_dir = f"tmp/job-{self.current_job['id']}/reducer-output"
                reduce_job = {
                    "message_type": "new_worker_job",
                    "input_files": self.task_queue[0],
                    "executable": self.current_job['reducer_executable'],
                    "output_directory": out_dir,
                    "worker_pid": pid
                }
                utils.send_tcp_message(worker_info['worker_port'], reduce_job)
                worker_info['current_task'] = self.task_queue[0]
                self.task_queue.pop(0)
                worker_info['status'] = 'busy'

    def present_output(self):
        """Move final files from reducer output dir to user's output dir."""
        reduced_dir = f"tmp/job-{self.current_job['id']}/reducer-output"
        reduced_files = os.listdir(Path(reduced_dir))
        output_dir = self.current_job['output_directory']
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        file_num = 1
        for file_name in reduced_files:
            file_num_str = f'0{file_num}' if file_num < 10 else f'{file_num}'
            new_name = f'outputfile{file_num_str}'
            new_path_str = f'{output_dir}/{new_name}'
            reduced_path_str = f'{reduced_dir}/{str(file_name)}'
            shutil.copyfile(reduced_path_str, new_path_str)
            file_num += 1


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    """Declare instance of master on given port."""
    Master(port)


if __name__ == '__main__':
    main()
