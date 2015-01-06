#-*- coding: utf8 -*-

from os.path import getsize as os_path_getsize
from hashlib import md5
from sys import exit
from threading import Thread

class FileSliceThread(Thread):
    def __init__(self, filepath, partfilepath, partnum, partsize, partmd5, threadlock, buffersize=8192):
        """
            @params filepath: file to be sliced
            @params partfilepath: partfile path
            @params partnum: partfile num
            @params partsize: partfile size
            @params partmd5: partfile md5 hexdigest
            @params threadlock: thread lock
            @params buffersize: read buffer size
        """ 
        Thread.__init__(self)
        self.filepath = filepath
        self.partfilepath = partfilepath
        self.partnum = partnum
        self.partsize = partsize
        self.partmd5 = partmd5
        self.threadlock = threadlock
        self.buffersize = buffersize
        self.md5 = md5()

    def _slice(self):
        filesize = os_path_getsize(self.filepath)
        startpos = self.partnum * self.partsize
        if startpos + self.partsize > filesize:
            endpos = filesize
        else:
            endpos = startpos + self.partsize
        with open(self.partfilepath, 'wb') as writer:
            with open(self.filepath, 'rb') as reader:
                reader.seek(startpos)
                while startpos < endpos:
                    if startpos + self.buffersize > endpos:
                        readsize = endpos - startpos
                    else:
                        readsize = self.buffersize
                    data = reader.read(readsize)
                    self.md5.update(data)
                    writer.write(data)
                    startpos += readsize
        ##
        # compare correct partmd5 with calculated partmd5
        if self.md5.hexdigest() != self.partmd5:
            with self.threadlock:
                print('Partnum {num} MD5 is NOT EQUAL: {correct} != {cal}'.format(\
                                                                num=self.partnum,\
                                                                correct=self.partmd5,\
                                                                cal=self.md5.hexdigest()))
                exit(1)

    def run(self):
        self._slice()