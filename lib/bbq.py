#!/usr/bin/python

import os
import uuid
import time
import glob
import StringIO

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

def _generate_message_id(priority=4):
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

    def enqueue(self, message, priority=4):
        """Enqueues the passed message with the given priority."""
        self._enqueue(message, priority)

    def list(self):
        """Lists all of the messages in the queue in the passed format."""
        messages = os.listdir(self.cur)
        messages.sort()
        return messages

    def claim(self, message_id):
        """Claims the passed message for the current process.  If the message cannot be
        claimed, return an error."""
        pass

    def unclaim(self, message_id):
        """Unclaims the passed message, returning it to the pool of messages that can be
        claimed by other processes."""
        pass

    def _check_authority(self, message_id):
        """Ensures that the current process owns the passed message."""

    def read(self, message_id):
        """Reads the contents of the passed message."""
        # TODO: Locate the message, it might be in cur/ or clm.
        buffer = StringIO.StringIO()
        path = os.path.join(self.cur, message_id)
        file = open(path, "r")
        while True:
            line = file.readline()
            if (line == ''):
                break
            buffer.write(line)
        file.close()
        return buffer.getvalue()
        
    def get_message_id(self, uuid):
        """Finds a message by the unique id component of the message id, returning the message text."""
        files = glob.glob("%s/*%s*" % (self.cur, uuid))
        if len(files) == 0:
            return None
        elif len(files) > 1:
            raise "Inconsistent index, more than one message has the same unique id."
        else:
            message_id = files[0]
            return message_id
        

