#!/usr/bin/env python
# -*- coding: utf8 -*-

from os.path import exists as os_path_exists,\
                    dirname as os_path_dirname,\
                    join as os_path_join
from os import remove as os_remove
from sys import argv, exit, stdout
from hashlib import md5
from cPickle import load as pickle_load

def makeup_file_slices(indexfile):
    # get metaData stored in indexfile
    with open(indexfile, 'rb') as reader:
        metaData = pickle_load(reader)
    dirname = os_path_dirname(indexfile)
    filename = metaData['whole'].get('name')
    md5sumOfWholeFile = metaData['whole'].get('md5sum')
    fileslices = metaData['slices']
    calMD5OfWholeFile = md5()
    buffersize = 1024 * 1024
    finished = 0
    # start to make up file slices
    filepath = os_path_join(dirname, filename)
    with open(filepath, 'wb') as writer:
        for partfile in sorted(fileslices):
            partfilepath = os_path_join(dirname, partfile)
            # if any partfile is not found
            # then exit immediately
            if not os_path_exists(partfilepath):
                print('No such file: {partfile}'.format(\
                                        partfile=partfile))
                exit(1)
            # calculate md5sum of each partfile
            # then compare with the correct md5sum
            correctMd5sum = fileslices[partfile]
            calMD5= md5()
            with open(partfilepath, 'rb') as reader:
                while True:
                    data = reader.read(buffersize)
                    if not data:
                        break
                    writer.write(data)
                    calMD5.update(data)
                    calMD5OfWholeFile.update(data)
            # if both are not the same
            # then we remove the file which is in progress
            if correctMd5sum != calMD5.hexdigest():
                os_remove(filepath)
                print('\nPartfile {path} MD5 is NOT EQUAL: {correct} != {cal}'.format(\
                                                            path=partfilepath,\
                                                            correct=correctMd5sum,\
                                                            cal=calMD5.hexdigest()))
                exit(1)
            finished += 1
            progress = int(100.0 * finished / len(fileslices))
            print "\rProgress: {progress}%".format(progress=progress),
            stdout.flush()
    if md5sumOfWholeFile == calMD5OfWholeFile.hexdigest():
        print('\nSucceed! MD5 is EQUAL: {md5sum}'.format(md5sum=md5sumOfWholeFile))
    else:
        print('\nWholefile MD5 is NOT EQUAL: {correct} != {cal}'.format(\
                                            correct=md5sumOfWholeFile,\
                                            cal=calMD5OfWholeFile.hexdigest()))

def main():
    Usage = 'Usage: {name} [indexfile]'.format(name=argv[0])
    if len(argv) != 2:
        print Usage
        exit(1)
    else:
        indexfile = argv[1]
        if not os_path_exists(indexfile):
            print('No such file: {indexfile}'.format(\
                                        indexfile=indexfile))
            exit(1)
        makeup_file_slices(indexfile)

if __name__ == '__main__':
    main()