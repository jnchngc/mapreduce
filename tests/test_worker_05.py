"""See unit test function docstring."""

import os
import json
from pathlib import Path
import utils
import mapreduce
from utils import TESTDATA_DIR


def master_message_generator(mock_socket):
    """Fake Master messages."""
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 3001,
        "worker_pid": os.getpid(),
    }).encode('utf-8')
    yield None

    # Simulate master creating output directory for worker
    os.mkdir("tmp/test_worker_05/output")

    # New map job
    yield json.dumps({
        'message_type': 'new_worker_job',
        'executable': TESTDATA_DIR/'exec/wc_map.sh',
        'input_files': [
            TESTDATA_DIR/'input/file01',
            TESTDATA_DIR/'input/file02',
        ],
        'output_directory': "tmp/test_worker_05/output/",
        'worker_pid': os.getpid(),
    }, cls=utils.PathJSONEncoder).encode('utf-8')
    yield None

    # Wait for worker to finish map job
    utils.wait_for_status_finished_messages(mock_socket)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_worker_05_map_two_inputs(mocker):
    """Verify worker correctly completes a map job with two input files.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    utils.create_and_clean_testdir("test_worker_05")

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.Mock()
    mockclientsocket.recv.side_effect = master_message_generator(mock_socket)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Run student worker code.  When student worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            3000,  # Master port
            3001,  # Worker port
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker, excluding heartbeat messages
    all_messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3001,
            "worker_pid": os.getpid(),
        },
        {
            "message_type": "status",
            "output_files": [
                "tmp/test_worker_05/output/file01",
                "tmp/test_worker_05/output/file02"
            ],
            "status": "finished",
            "worker_pid": os.getpid(),
        },
    ]

    # Verify final output
    with Path('tmp/test_worker_05/output/file01').open() as infile:
        actual01 = infile.readlines()
    with Path('tmp/test_worker_05/output/file02').open() as infile:
        actual02 = infile.readlines()
    assert sorted(actual01 + actual02) == sorted([
        '\t1\n',
        '\t1\n',
        'hello\t1\n',
        'hadoop\t1\n',
        'goodbye\t1\n',
        'hadoop\t1\n',
        'hello\t1\n',
        'bye\t1\n',
        'world\t1\n',
        'world\t1\n',
    ])
