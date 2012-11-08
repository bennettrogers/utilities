import os, sys, argparse, random, subprocess, shutil
import time

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
        outfile = os.path.join(outdir, '%s.txt' % (i,))
        num_bytes = random.expovariate(1.5) * 1024
        cmd = ['dd', 'if=/dev/urandom', 'of=%s' % (outfile,), 'bs=1024', 'count=%d' % (num_bytes,)]
        subprocess.call(cmd)

def transfer_files(mode, source_host, local_dir, numtrials):
    if not os.path.isdir(local_dir):
        os.mkdir(local_dir)
    for filename in os.listdir(local_dir):
        os.remove(os.path.join(local_dir, filename))
    if mode == 'nfs':
        remote_dir = os.path.join('/mnt/nfs-filestore/', source_host)
        with Timer() as t:
            for _ in xrange(numtrials):
                for filename in os.listdir(remote_dir):
                    filepath = os.path.join(remote_dir, filename)
                    shutil.copy(filepath, local_dir)
    #get some statistics
    numfiles = len([filename for filename in os.listdir(local_dir)])
    batchsize = sum([os.path.getsize(os.path.join(local_dir, filename)) for filename in os.listdir(local_dir)])
    stats = '''
    %d trials
    %d bytes per trial 
    %d files per trial
    %d bytes total
    %d files total
    %.03f seconds total
    ''' % (numtrials, batchsize, numfiles, numtrials*batchsize, numtrials*numfiles, t.interval,)

    print stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--transfer', action='store_true')
    parser.add_argument('--mode')
    parser.add_argument('--host')
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
        local_dir = args.outdir or os.path.relpath('outfiles')
        transfer_files(mode, source_host, local_dir, numtrials)
