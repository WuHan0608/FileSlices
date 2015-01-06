#!/usr/bin/env python2
#-*- coding: utf8 -*-

from sys import argv, stdout
from hashlib import md5
from cPickle import dump as pickle_dump
from time import sleep
from os.path import exists as os_path_exists,\
                    getsize as os_path_getsize
from threading import Lock
from FileSliceThread import FileSliceThread

def get_partmd5(filepath, partsize, buffersize=8192):
    filesize = os_path_getsize(filepath)
    partnums = filesize / partsize + 1
    allpartmd5 = {} # {part1num: part1md5, ...}
    md5OfWholeFile = md5()
    with open(filepath, 'rb') as reader:
        for num in xrange(partnums):
            partmd5 = md5()
            startpos = num * partsize
            if startpos + partsize > filesize:
                endpos = filesize
            else:
                endpos = startpos + partsize
            reader.seek(startpos)
            while startpos < endpos:
                if startpos + buffersize > endpos:
                    readsize = endpos - startpos
                else:
                    readsize = buffersize
                data = reader.read(readsize)
                partmd5.update(data)
                md5OfWholeFile.update(data)
                startpos += readsize
            allpartmd5[num] = partmd5.hexdigest()
    return (allpartmd5, md5OfWholeFile.hexdigest())

def iter_items(items, limit=3):
    ret = []
    for item in items:
        ret.append(item)
        if len(ret) == limit:
            yield ret
            ret = []
    if len(ret) > 0:
        yield ret

def main():
    Usage = 'Usage: {name} [filepath]'.format(name=argv[0])
    if len(argv) != 2:
        print Usage
        exit(1)
    else:
        filepath = argv[1]
        if not os_path_exists(filepath):
            print('No such file: {filepath}'.format(filepath=filepath))
            exit(1)
        partsize = 100 * 1024 * 1024  # set partsize to 100MB
        allpartmd5, md5OfWholeFile = get_partmd5(filepath, partsize)
        partnums = len(allpartmd5)
        allpartmd5 = [(partnum, partmd5) for partnum, partmd5 in allpartmd5.iteritems()]
        partfiles = []
        threadlock = Lock()
        threads = []
        finished = 0
        for item in iter_items(allpartmd5):
            for partnum, partmd5 in item:
                partfilepath = '{filepath}_{num:02d}'.format(filepath=filepath,num=partnum)
                partfiles.append((partfilepath, partmd5))
                thread = FileSliceThread(filepath, partfilepath, partnum, partsize, partmd5, threadlock)
                thread.start()
                threads.append(thread)
        # write metadata into index file
        metaData = {
            'whole': {'name': filepath, 'md5sum': md5OfWholeFile},
            'parts': partfiles
        }
        indexfile = '{filepath}.idx'.format(filepath=filepath)
        with open(indexfile, 'wb') as writer:
            pickle_dump(metaData, writer, protocol=2)
        # wait threads complete
        for thread in threads:
            with threadlock:
                thread.join
                finished += 1
                progress = int(100.0 * finished / partnums)
                print '\rProgress: {progress}%'.format(progress=progress),
                stdout.flush()
                sleep(0.1)

if __name__ == '__main__':
    main()