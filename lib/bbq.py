#!/usr/bin/python

import os
import uuid
import time

def is_queue(directory):
    """Determines if the passed directory is a valid queue."""
    directory = os.path.abspath(directory)
    tmp = os.path.join(directory, 'tmp')
    cur = os.path.join(directory, 'cur')
    clm = os.path.join(directory, 'clm')
    return os.path.exists(directory) and os.path.exists(tmp) and os.path.exists(cur) and os.path.exists(clm)    

def init(directory):
    """Creates a new queue in the passed directory."""
    Queue(directory, init=True)

def _generate_message_id(priority=5):
    """Generates a new message id that is guaranteed to be unique.  No
    checks are done to ensure that the path does not already exist."""
    
    return "%s.%s.%s.%s" % (priority, str(int(time.time())), uuid.uuid1(), os.getpid())

def parse_message_id(message_id):
    """Parses the given message id and returns a dictionary containing
    its components.  The dictionary will contain the following keys:

       priority - The priority of the message.
       created - A unix timestamp representing when the message was enqueued.
       id - the unique ID of the message.
       pid - PID of the process that enqueued the message."""

    components = os.path.basename(message_id).split('.')
    return { 'priority': int(components[0]), 
             'created': time.gmtime(float(components[1])),
             'id': components[2],
             'pid': int(components[3]) }
            

class Queue:
    
    directory = None
    tmp = None
    cur = None
    clm = None

    def __init__(self, directory, init=True):
        self.directory = os.path.abspath(directory)
        self.tmp = os.path.join(self.directory, 'tmp')
        self.cur = os.path.join(self.directory, 'cur')
        self.clm = os.path.join(self.directory, 'clm')

        if (init):
            self.init();
    
    def init(self):
        """Creates all of the necessary directories within the given queue directory.  If
        any of the operations fails an exception will be thrown."""
        if not os.path.exists(self.tmp):
            os.makedirs(self.tmp, 0777)

        if not os.path.exists(self.cur):
            os.makedirs(self.cur, 0777)

        if not os.path.exists(self.clm):
            os.makedirs(self.clm, 0777)

    def _dequeue(self):
        # find a file that needs to be dequeued
        os.listdir(self.cur)

    def dequeue(self):
        pass

    def _enqueue(self, message, priority=5):
        attempts = 0
        while True:
            if (attempts > 10):
                break;

            message_id = _generate_message_id(priority)
            cur_path = os.path.join(self.cur, message_id)
            tmp_path = os.path.join(self.tmp, message_id)

            if (not os.path.exists(tmp_path) and not os.path.exists(cur_path)):
                break

            attempts = attempts + 1

        file = open(tmp_path, "w")
        file.write(message)
        file.close()

        os.link(tmp_path, cur_path)
        os.unlink(tmp_path)

    def enqueue(self, message, priority=5):
        """Enqueues the passed message with the given priority."""
        self._enqueue(message, priority)

    def list(self):
        """Lists all of the messages in the queue in the passed format."""
        messages = os.listdir(self.cur)
        messages.sort()
        return messages


