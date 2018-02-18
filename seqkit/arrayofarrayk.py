#!/usr/bin/env python
"""
time python ./arrayofarrayk.py S_lycopersicum_chromosomes.2.50.fa.gz
Num Kmers       737,528,050
Num Uniq Kmers          512
real    9m00.589s
user    8m57.078s
sys     0m03.313s
pigz
real    8m38.941s
user    8m38.828s
sys     0m3.531s

time pypy3 ./arrayofarrayk.py S_lycopersicum_chromosomes.2.50.fa.gz
Num Kmers       737,528,050
Num Uniq Kmers          512
real    0m39.627s
user    0m36.906s
sys     0m02.375s
pigz
real    0m29.580s
user    0m31.438s
sys     0m2.969s

time ./seqkit kmer ../../../S_lycopersicum_chromosomes.2.50.fa.gz
Num Kmers      737,528,493
Num Uniq Kmers         512
real    0m09.001s
user    0m06.781s
sys     0m06.016s




time ./seqkit kmer ../../../S_lycopersicum_chromosomes.2.50.fa.gz
               WITHOUT SEARCH |         WITH SEARCH
                         COPY |        COPY       POINTER
real                7m17.361s |   1m40.241s     1m35.382s
user                7m52.531s |   1m47.531s     1m42.625s
sys                 5m55.500s |   1m19.031s     1m14.813s
[INFO] Size       823,944,041 | 823,944,041   823,944,041
[INFO] Registers           13 |          13            13
[INFO] Lines                0 |           0             0
[INFO] Chars      823,944,041 | 823,944,041   823,944,041
[INFO] Valids     737,636,348 | 737,636,348   737,636,348
[INFO] Counted    737,528,493 | 737,528,493   737,528,493
[INFO] Skipped              0 |           0             0
[INFO] Resets      86,307,693 |  86,307,693   86,307,693



time ./seqkit kmer -k 11 ../../../S_lycopersicum_chromosomes.2.50.fa.gz
               WITHOUT SEARCH |         WITH SEARCH
                         COPY |        COPY       POINTER
real               91m38.008s |  60m59.557s    66m48.944s
user              100m23.047s |  67m29.797s    73m37.469s
sys                78m28.188s |  52m41.609s    55m52.094s
[INFO] Size       823,944,041 | 823,944,041   823,944,041
[INFO] Registers           13 |          13            13
[INFO] Lines                0 |           0             0
[INFO] Chars      823,944,041 | 823,944,041   823,944,041
[INFO] Valids     737,636,348 | 737,636,348   737,636,348
[INFO] Counted    737,366,752 | 737,366,752   737,366,752
[INFO] Skipped              0 |           0             0
[INFO] Resets      86,307,693 |  86,307,693    86,307,693


                variable   fixed
/tmp/t.kmer.gz    100412  138724 72%




-k5 -j 1
num kmers:          512 last kmer:          512 len kmer:          512 cap kmer:    1,000,000
real    2m43.211s
user    2m42.094s
sys     0m0.828s

-k5 -j 2
num kmers:          983 last kmer:          983 len kmer:          983 cap kmer:    1,000,000
real    4m12.503s
user    6m17.344s
sys     1m24.422s

-k5 -j 4
num kmers:          955 last kmer:          955 len kmer:          955 cap kmer:    1,000,000
real    3m15.581s
user    4m48.688s
sys     0m9.719s

-k5 -j 8
num kmers:          971 last kmer:          971 len kmer:          971 cap kmer:    1,000,000
real    3m11.483s
user    4m52.547s
sys     0m12.422s



-k7 -j 1
num kmers:        8,192 last kmer:        8,192 len kmer:        8,192 cap kmer:    1,000,000
real    3m46.348s
user    3m45.016s
sys     0m0.766s

-k7 -j 2
num kmers:        8,192 last kmer:        8,192 len kmer:        8,192 cap kmer:    1,000,000
real    5m29.863s
user    7m39.469s
sys     1m25.125s

-k7 -j 4
num kmers:        8,192 last kmer:        8,192 len kmer:        8,192 cap kmer:    1,000,000
real    4m26.460s
user    6m0.625s
sys     0m10.266s

-k7 -j 8
num kmers:        8,192 last kmer:        8,192 len kmer:        8,192 cap kmer:    1,000,000
real    4m30.807s
user    6m14.781s
sys     0m12.078s


"""
import sys
import gzip

def openfile(filename, mode):
    if filename == '-':
        print( "reading from stdin" )
        return sys.stdin.buffer
    else:
        if filename.endswith(".gz"):
            return gzip.open(filename, mode+"b")
        else:
            return open(filename, mode)

def toBin(num, chunk_size=8):
    bv = "{:064b}".format(num)
    re = " ".join([bv[i:i+chunk_size] for i in range(0, len(bv), chunk_size)])
    return re

class Conv(object):
    def __init__(self, kmerlen):
        self.kmerlen = kmerlen
        self.vals    = None
        self.conv    = None
        self.res     = None
        self.init()
    
    def init(self):
        self.chars   = (ord('A'), ord('C'), ord('G'), ord('T'))
        self.vals    = [None] * 256
        self.cleaner = (1 << ((self.kmerlen)*2)) - 1
        self.res     = [0] * self.cleaner
        print( "cleaner {} {:12,d} - {}".format(" "*22,     self.cleaner, toBin(    self.cleaner) ) )
        print( "res     {} {:12,d} - {}".format(" "*22, len(self.res)   , toBin(len(self.res)   ) ) )
        
        for v, c in enumerate(self.chars):
            self.vals[ c ] = v
        
        # self.conv = []
        # for pos in range(self.kmerlen):
        #     self.conv.append([None] * 256)
        #     for v, c in enumerate(self.chars):
        #         r = v << (pos*2)
        #         self.conv[pos][c] = r
        #         print( "POS {:4d} VAL {} CHAR {} ({}) RES {:12,d} - {}".format(pos, v, c, chr(c), r, toBin(r)) )

    def printStats(self):
        # for i, v in enumerate(self.res):
        #     print("{:12,d} {:12,d} {}".format(i,v,toBin(i)))

        print( "Num Kmers      {:12,d}".format(sum(self.res)) )
        print( "Num Uniq Kmers {:12,d}".format(sum([1 for x in self.res if x > 0])) )

    def __call__(self, nam, lst):
        print( "parsing {}".format( nam ) )
        #ctn = b"".join(seq)
        vals    = self.vals
        cleaner = self.cleaner
        kmerlen = self.kmerlen
        res     = self.res
        val     = 0
        lav     = 0
        curr    = 0
        count   = 0
        for seq in lst:
            for s in seq:
                count += 1
                v      = vals[ s ]
                
                if v is None:
                    curr = 0
                    val  = 0
                    lav  = 0
                    continue
                
                w      = 3 - v
                # print( "v       {} {:12,d} - {} - CHAR {} - CURR {} COUNT {:12d}".format(" "*22, v, toBin(v), chr(s), curr, count ) )
                # print( "w       {} {:12,d} - {} - CHAR {} - CURR {} COUNT {:12d}".format(" "*22, w, toBin(w), " "   , curr, count ) )
                val <<= 2
                val  &= cleaner
                val  += v
                lav >>= 2
                lav  += w << (2*(kmerlen-1))
                # print( "val     {} {:12,d} - {}".format(" "*22, val, toBin(val) ) )
                # print( "lav     {} {:12,d} - {}".format(" "*22, lav, toBin(lav) ) )
                if curr == kmerlen - 1:
                    # print( "val     {} {:12,d} - {}".format(" "*22, val, toBin(val) ) )
                    # print( "lav     {} {:12,d} - {}".format(" "*22, lav, toBin(lav) ) )
                    # print()
                    if lav < val:
                        res[lav] += 1
                    else:
                        res[val] += 1
                    pass
                else:
                    # print(".")
                    curr += 1
                


def parseSeq(filename, conv):
    seq = []
    nam = None
    with openfile(filename, 'r') as fhd:
        for line in fhd:
            line = line.strip()
            
            if len(line) == 0:
                continue
            
            # print( line )
            
            if line[0] == ord(">"):
                # print( "IS  > :: ", line[0], line )
                # print( line )
                if len(seq) != 0:
                    sys.stdout.write(' {:12,d}\n'.format(len(seq)) )
                    conv( nam, seq )
                    del seq[:]
                    # break

                nam = line[1:]
                print( "NAME", nam )
                
            else:
                # print( "NOT > :: ", line[0], line )
                assert nam is not None
                seq.append(line)
                if len(seq) % 100000 == 0:
                    sys.stdout.write(' {:12,d}\n'.format(len(seq)))
                    sys.stdout.flush()
            
        if len(seq) != 0:
            conv( nam, seq )

def main(filename, kmerlen=5):
    conv = Conv(kmerlen)
    parseSeq( filename, conv )
    conv.printStats()

if __name__ == "__main__":
    main(sys.argv[1])