#!/usr/bin/env python

import argparse
import storage
import os


def checkstor(path):
    if path.startswith('s3://'):
        path = path[len('s3://'):]

        if '/' in path:
            bucket, prefix = path.split('/', 1)
        else:
            bucket, prefix = path, '.'

        stor = storage.S3Storage(args.s3_endpoint,
                                 bucket,
                                 prefix,
                                 args.s3_access_key_id,
                                 args.s3_secret_access_key,
                                 args.s3_region)

    else:
        stor = storage.FilesystemStorage(path)
        
    return stor


def main():
    global args
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--s3-access-key-id', help='access key for connecting to S3')
    parser.add_argument(
        '--s3-secret-access-key', help='secret key for connecting to S3')

    parser.add_argument(
        '--s3-endpoint',
        help='region endpoint for connecting to S3 (default: s3.amazonaws.com)')

    parser.add_argument(
        '--s3-region',
        help='S3 region name')
    parser.add_argument(
        'from_path',
        help='File source. Either s3://bucket/prefix or /path/on/local/fs')

    parser.add_argument(
        'to_path',
        help='File destination. Either s3://bucket/prefix or /path/on/local/fs')
        
    args = parser.parse_args()

    from_path = args.from_path
    from_stor = checkstor(from_path)
    to_path = args.to_path
    to_stor = checkstor(to_path)
    
    print('From: {}'.format(from_path))        
    print('To: {}'.format(to_path))

    for item in from_stor.files():
        if item == '/': continue
        if from_path[-1] != '/' and item[0] != '/':
            new_from_path = from_path+'/'+item
        else:
            new_from_path = from_path+item
            
        if to_path[-1] != '/' and item[0] != '/':
            new_to_path = to_path+'/'+item
        else:
            new_to_path = to_path+item

        if to_stor.exists(item):            
            if from_stor.mtime(item) < to_stor.mtime(item):
                continue        
                
        try:
            temp = from_stor.read_file(item)
            to_stor.write_file(item,temp)
            print('{} > {} ({} bytes)'.format(new_from_path,new_to_path,len(temp)))
        except Exception as e:
            print('{} > {} (Error: {})'.format(new_from_path,new_to_path,e))
            
    


if __name__ == '__main__':
    main()
