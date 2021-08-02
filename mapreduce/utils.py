"""Utils file.

This file is to house code common between the Master and the Worker

"""
import socket
import json
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)


def create_socket(is_tcp):
    """Create either a UDP or TCP socket for communication."""
    if is_tcp:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return sock


def send_message(sock, message_dict):
    """Send message to specified socket."""
    # logging.debug(json.dumps(message_dict, indent=2))
    message = json.dumps(message_dict)
    sock.sendall(message.encode('utf-8'))


def send_tcp_message(port, message):
    """Send a message to specified port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.connect(("localhost", port))
    message_str = json.dumps(message)
    sock.sendall(message_str.encode('utf-8'))
    sock.close()
    # print the message sent
    # logging.debug(
    #     "\n Sending \n %s \n\n",
    #     json.dumps(message, indent=2)
    # )
