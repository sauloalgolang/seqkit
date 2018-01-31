#!/usr/bin/env python
"""
time python ./arrayofarrayk.py S_lycopersicum_chromosomes.2.50.fa.gz
Num Kmers       737,528,050
Num Uniq Kmers          512
real    9m00.589s
user    8m57.078s
sys     0m03.313s

time pypy3 ./arrayofarrayk.py S_lycopersicum_chromosomes.2.50.fa.gz
Num Kmers       737,528,050
Num Uniq Kmers          512
real    0m39.627s
user    0m36.906s
sys     0m02.375s

time ./seqkit kmer ../../../S_lycopersicum_chromosomes.2.50.fa.gz
Num Kmers      737,528,493
Num Uniq Kmers         512
real    0m09.001s
user    0m06.781s
sys     0m06.016s
"""
import sys
import gzip

def openfile(filename, mode):
    if filename == '-':
        print "reading from stdin"
        return sys.stdin
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