#!/usr/bin/python

import os
import uuid
import time
import glob
import StringIO

def is_queue(directory):
    
        tmp = os.path.join(directory, 'tmp')
        cur = os.path.join(directory, 'cur')
        clm = os.path.join(directory, 'clm')
        if os.path.exists(tmp) and \
                os.path.exists(cur) and \
                os.path.exists(clm):
            return True
        else:
            return False

def generate_message_id():
    return "%s" % (str(uuid.uuid1()).replace("-", ""))

class Message:
    """Represents a message in the queue.  The <code>message</code> property of
    this class contains the message payload."""
    def __init__(self, message):
        self.message = message;

class ClaimedError:

    def __init__(self, key):
        self.key = key

class QueueDir:
    """A queue in Queuedir format.
    
    TOOD: This class should implement the operator-overloaded methods as mailbox.Mailbox.
    """

    def __init__(self, directory):
        self.directory = directory;
        self.tmp = os.path.join(self.directory, 'tmp')
        self.cur = os.path.join(self.directory, 'cur')
        self.clm = os.path.join(self.directory, 'clm')
        self._initialize_queuedir()
        
    def _initialize_queuedir(self):
        """Creates all of the required directories under the instance's root directory."""
        if not os.path.exists(self.tmp):
            os.makedirs(self.tmp, 0777)

        if not os.path.exists(self.cur):
            os.makedirs(self.cur, 0777)

        if not os.path.exists(self.clm):
            os.makedirs(self.clm, 0777)
        
    def add(self, message):
        """Adds the passed message to the queue.  The message should be an instance of
        bbq.Message.  Other formats may be accepted later."""
        
        attempts = 0
        while True:
            if (attempts > 10):
                break
                
            id = generate_message_id()
            cur_path = os.path.join(self.cur, id)
            tmp_path = os.path.join(self.tmp, id)

            if (not os.path.exists(tmp_path) and not os.path.exists(cur_path)):
                break

            attempts = attempts + 1

        file = open(tmp_path, "w")
        file.write(message.message)
        file.close()

        try:
            os.link(tmp_path, cur_path)
            os.unlink(tmp_path)
        except OSError:
            # On AFP, os.link does not work, so instead we have to fall back to renaming
            # which has roughly the same effect.
            os.rename(tmp_path, cur_path)
        
    def iterkeys(self):
        """Returns an iterator over the message keys."""
        pass
    
    def keys(self, include_locked = False):
        """Returns a list containing all of the messages in the queue that have not been locked, or have
        been locked by the current process.  If a list of all messages regardless of their locked status
        is needed, pass True for include_locked.

        Due to the nature of a queue, the list may be out of date by the time the results are returned
        and used."""
        
        cur = glob.glob(os.path.join(self.cur, "*"))

        pid = os.getpid()
        if include_locked:
            pid = "*"
        pattern = os.path.join(self.clm, "*-%s-*" % (pid))
        clm = glob.glob(pattern)
        
        messages = [ os.path.basename(path).split("-")[0] for path in  cur + clm ]
        return messages
    
    
    def remove(self, key, token=os.getpid(), force=False):
        """Removes the message with the passed key."""

        if force:
            token = "*"
        pattern = os.path.join(self.clm, "%s-%s-*" % (key, token))
        matches = glob.glob(pattern)
        if len(matches) == 1:
            clm_path = matches[0]
            try:
                os.unlink(clm_path)
                return True
            except OSError:
                return False
        
        cur_path = os.path.join(self.cur, key)
        try:
            os.unlink(cur_path)
            return True
        except OSError:
            return False
        

    def get(self, key, token=os.getpid()):
        """Return a message with the passed key, or None if the message cannot be found or cannot be locked.
        If no message message exists a KeyError
        exception is raised."""

        message = None
        
        self.lock(key)
        
        pattern = os.path.join(self.clm, "%s-%s-*" % (key, token))
        matches = glob.glob(pattern)
        
        if len(matches) == 1:
            f = open(matches[0], "r")
            data = "".join(f.readlines())
            message = Message(data)
            f.close()

        self.unlock(key)

        return message
    
    def lock(self, key, token=os.getpid()):
        """Claims the message with the passed key."""
        
        # Claiming a message consists of moving the file from the cur directory
        # into the clm directory.  During the move, the file also needs to be appended
        # with a unix timestamp, in UTC, of when the message was claimed.  Having the
        # timestamp means that other well behaved Queuedir queues can unclaim the message
        # if the message has not been processed by its owner by the specified time.
        #
        # If the above operation fails, the exact cause can be determined by stating
        # looking through the contents of the cur and clm directory.  If there is no file
        # in the cur directory with the same name as the key, and there is no file in the
        # clm directory that starts with the key then either the message existed and has
        # been processed, or the message never existed in the first place.  If ...
        
        cur_path = os.path.join(self.cur, key)
        clm_path = os.path.join(self.clm, "%s-%s-%s" % (key, token, str(int(time.time()))))
        
        try:
            os.rename(cur_path, clm_path)
        except OSError:
            return False

        return True
 
    def unlock(self, key, token=os.getpid(), force=False):
        """Returns a bbq.Message containing the next message in the queue to be worked.
        If there is no next item this method returns None."""

        # Build up the globbing pattern to find the message in the queue.  Ordinarily
        # we want to ensure that we only unlock messages that are locked by this process.
        # However, if we want to forcibly unlock a message we can ignore the pid
        # in the globbing pattern.
        if (force):
            token = "*"
        pattern = os.path.join(self.clm, "%s-%s-*" % (key, token))
        matches = glob.glob(pattern)
        if len(matches) == 0 or len(matches) > 1:
            return False
        
        clm_path = matches[0]
        cur_path = os.path.join(self.cur, key)

        try:
            os.rename(clm_path, cur_path)
            return True
        except OSError:
            # If there was a problem renaming the file then either the message
            # has already been deleted, or it's been unlocked by another process.
            return False

    def stat(self, key):
        """Returns information about a particular message.  The result is a tuple
        of (key, token, created date, expiry date).  If the message cannot be stat'd
        an empty tuple will be returned.
        """
        
        # TODO: Instead of using globbing across both cur and clm, first check
        # the cur directory with a direct lookup.  If that fails, use globbing in
        # clm, which should be a smaller directory.  This will speed up this operation
        # immensely.        
        pattern = os.path.join(self.directory, "*/%s*" % key)
        matches = glob.glob(pattern)
        if len(matches) == 1:
            match = matches[0]
            # We add "--" to the end of the file name so that the destructuring
            # bind will succeed even if the file has not been locked.
            (key, token, etime) = (os.path.basename(match) + "--").split("-")[:3]
            
            if etime:
                etime = int(etime)
            else:
                etime = ''
                
            try:
                ctime = int(os.stat(match).st_ctime)
            except OSError:
                return ()
            return (key, token, ctime, etime)
