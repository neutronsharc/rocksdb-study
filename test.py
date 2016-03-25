#!/usr/bin/python

import sys
import glob
sys.path.append('gen-py')

import urllib2
import os 
import time

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from rocksdb.replication import Replication
from rocksdb.replication.ttypes import *


def main():
    print 'test thrift python client'
    address = 'localhost'
    socket = TSocket.TSocket(address, 9995)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Replication.Client(protocol);
    
    try:
        transport.open()
        ckptname = 'testckpt'
        resp = client.CreateCheckpoint(ckptname)
        if resp.result != ErrorCode.SUCCESS:
            print 'faile to create ckpt {}'.format(ErrorCode._VALUES_TO_NAMES[resp.result])
        else:
            print 'ckpt files located in db dir: {}:\n'.format(resp.dirname)

        for name in resp.filenames:
            print '\t{}'.format(name)

        # http server's root dir is checkpoint dir.
        rootdir = resp.dirname
        httpinfo = client.StartHttpServer(rootdir)
        print 'http server info: {}'.format(httpinfo)


        # open local dir to save downloaded files.
        local_dir = ckptname
        if not os.path.exists(local_dir):
            os.mkdir(local_dir, 0755)

        for name in resp.filenames:
            if name == '.' or name == '..':
                continue
            remote_name = 'http://{}:{}/{}'.format(address, httpinfo.port, name)
            remotef = urllib2.urlopen(remote_name)
            local_filename = '{}/{}'.format(local_dir, name)
            with open(local_filename, 'wb') as output:
                output.write(remotef.read())
            print 'have downloaded file {}'.format(local_filename)

        # download files from http server.
        client.StopHttpServer(rootdir)
        time.sleep(1)

        client.DeleteCheckpoint(ckptname)
        time.sleep(1)

    except Exception as e:
        print 'got exception: {}'.format(e)


if __name__ == '__main__':
    main()



