import os, argparse, random, subprocess, shutil
import time
import requests

class Timer:
    def __enter__(self):
        self.start = time.time()
        return self
    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

def generate_files(count, outdir):
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    for i in xrange(count):
        outfile = os.path.join(outdir, '%s.file' % (i,))
        num_bytes = random.expovariate(1.5) * 1024
        cmd = ['dd', 'if=/dev/urandom', 'of=%s' % (outfile,), 'bs=1024', 'count=%d' % (num_bytes,)]
        subprocess.call(cmd)

def transfer_files(mode, source_host, local_dir, numtrials=1, keyfile=None):
    if not os.path.isdir(local_dir):
        os.mkdir(local_dir)
    for filename in os.listdir(local_dir):
        os.remove(os.path.join(local_dir, filename))
    if mode == 'nfs':
        remote_dir = os.path.join('/mnt/nfs-filestore/', source_host)
        file_list = [filename for filename in os.listdir(remote_dir)]
        with Timer() as t:
            for _ in xrange(numtrials):
                for filename in file_list:
                    filepath = os.path.join(remote_dir, filename)
                    shutil.copy(filepath, local_dir)
    elif mode == 'scp':
        remote_dir = '/mnt/scp-filestore'
        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-i', keyfile, 'ubuntu@%s' % (source_host,), 'find %s -type f' % (remote_dir,)]
        ssh = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        file_list = [filepath.strip() for filepath in ssh.stdout.readlines()]
        with Timer() as t:
            for _ in xrange(numtrials):
                for filepath in file_list:
                    scp_cmd = ['scp', '-o StrictHostKeyChecking=no', '-c', 'arcfour', '-i', keyfile, '%s:%s' % (source_host, filepath.strip(),), local_dir]
                    subprocess.call(scp_cmd)
    elif mode == 'tar':
        remote_dir = '/mnt/tar-filestore'
        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-c', 'arcfour', '-i', keyfile, 'ubuntu@%s' % (source_host,), 'find {0} -type f -printf "%f\n"'.format(remote_dir)]
        ssh = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        file_list = [filename.strip() for filename in ssh.stdout.readlines()]
        with Timer() as t:
            for _ in xrange(numtrials):
                for filename in file_list:
                    fetch_cmd = ['ssh', '-o StrictHostKeyChecking=no', '-i', keyfile, 'ubuntu@%s' % (source_host,), 'cd %s; tar cf - %s' % (remote_dir, filename,)]
                    write_cmd = ['tar', 'xf', '-', '-C', local_dir]
                    fetch_process = subprocess.Popen(fetch_cmd, stdout=subprocess.PIPE)
                    write_process = subprocess.Popen(write_cmd, stdin=fetch_process.stdout, stdout=subprocess.PIPE)
                    out, err = write_process.communicate()
    elif mode == 'http':
        file_list = [filename.strip() for filename in ssh.stdout.readlines()]
        with Timer() as t:
            for _ in xrange(numtrials):
                for filename in file_list:
                    remote_path = 'http://{0}/{1}'.format(source_host, filename,)
                    local_path = os.path.join(local_dir, filename)
                    response = requests.get(remote_path)
                    if not response.ok:
                        raise Exception('Error getting file from HTTP filestore')
                    with open(local_path, 'wb') as local_file:
                        for chunk in response.iter_content():
                            local_file.write(chunk)

    # print some statistics
    numfiles = len([filename for filename in os.listdir(local_dir)])
    batchsize = sum([os.path.getsize(os.path.join(local_dir, filename)) for filename in os.listdir(local_dir)])
    stats = '''
    %d trials
    %.02f MB per trial
    %d files per trial
    %.02f MB total
    %d files total
    %.03f seconds total
    %.03f MBps average transfer rate
    ''' % (numtrials, batchsize/1048576.0, numfiles, numtrials*batchsize/1048576.0, numtrials*numfiles, t.interval, numtrials*batchsize/1048576.0/t.interval)

    print stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--transfer', action='store_true')
    parser.add_argument('--mode')
    parser.add_argument('--host')
    parser.add_argument('--keyfile')
    parser.add_argument('--generate', action='store_true')
    parser.add_argument('--numfiles', type=int)
    parser.add_argument('--numtrials', type=int)
    parser.add_argument('--outdir')
    args = parser.parse_args()

    if args.generate:
        numfiles = args.numfiles or 100
        outdir = args.outdir or os.path.relpath('sourcefiles')
        generate_files(numfiles, outdir)
    elif args.transfer:
        mode = args.mode
        numtrials = args.numtrials or 1
        source_host = args.host
        keyfile = args.keyfile
        local_dir = args.outdir or os.path.relpath('outfiles')
        transfer_files(mode, source_host, local_dir, numtrials, keyfile)
