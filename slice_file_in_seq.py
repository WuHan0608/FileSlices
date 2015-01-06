#!/usr/bin/env python2
# -*- coding: utf8 -*-

from os.path import getsize as os_path_getsize,\
                    exists as os_path_exists,\
                    basename as os_path_basename
from hashlib import md5
from sys import argv, exit
from cPickle import dump as pickle_dump

def get_md5(filepath, md5obj, size=8192):
    """ Return md5 object """
    with open(filepath, 'rb') as reader:
        while True:
            data = reader.read(size)
            if not data:
                break
            md5obj.update(data)
    return md5obj

def slice_file(filepath, partsize):
    """ Split a file into multiple slices """
    filesize = os_path_getsize(filepath)
    startpos = 0  # file pointer start position, defaults to 0
    buffersize = 1024 * 1024 # file pointer move forward buffersize each time
    md5OfWholeFile = get_md5(filepath, md5())
    md5OfFileSlices = md5()
    metaData = {}
    # slice file into partfiles
    with open(filepath, 'rb') as reader:
        while startpos < filesize:
            # We use partsize num from 1 just for convenience
            for num in xrange(1, filesize / partsize + 2):
                partfile = '{filepath}_{num:02d}'.format(\
                                            filepath=filepath,\
                                            num=num)
                # endpos of each file slice
                # move file pointer to endpos for reading data chunk
                if partsize * num > filesize:
                    endpos = filesize
                else:
                    endpos = partsize * num
                md5sumOfSlice = md5() # store md5sum of each slice
                with open(partfile, 'wb') as writer:
                    while startpos < endpos:
                        if startpos + buffersize > endpos:
                            readsize = endpos - startpos
                        else:
                            readsize = buffersize
                        data = reader.read(readsize)
                        writer.write(data)
                        md5sumOfSlice.update(data)
                        startpos += readsize
                # store necessary data
                md5OfFileSlices = get_md5(partfile, md5OfFileSlices)
                partfilename = os_path_basename(partfile)
                metaDataOfSlice = {partfilename: md5sumOfSlice.hexdigest()}
                metaData.setdefault('slices', {}).update(metaDataOfSlice)
    #
    # compare md5sum of the whole file with md5sum of file slices
    # if both are the same then succeed to complete
    # otherwise failed
    if md5OfWholeFile.hexdigest() == md5OfFileSlices.hexdigest():
        md5sumOfWholeFile = md5OfWholeFile.hexdigest()
        filename = os_path_basename(filepath)
        metaData['whole'] = {'name': filename, 'md5sum': md5sumOfWholeFile}
        # pickle dump meta data into index file
        indexfile = filepath + '.idx'
        with open(indexfile, 'wb') as fp:
            pickle_dump(metaData, fp, protocol=2)
        print('Succeed! MD5 is EQUAL: {md5}'.format(md5=md5sumOfWholeFile))
    else:
        print('MD5 is NOT EQUAL, {wholefile} != {fileslices}'.format(\
                                                wholefile=md5OfWholeFile.hexdigest(),\
                                                fileslices=md5OfFileSlices.hexdigest()))

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
        partsize = 10 * 1024 * 1024 # set partsize to 10MB
        slice_file(filepath, partsize)

if __name__ == '__main__':
    main()