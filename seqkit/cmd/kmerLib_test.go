package cmd

// go test -v

import (
        "testing"
)

func TestSort(t *testing.T) {
    var kmerSize uint64 = 2
    var res       = NewKmerHolder(kmerSize)
	res.KmerSize  = kmerSize
	res.KmerCap   =       3
	res.BufferCap =       2
	res.Kmer      = make(KmerArr, 0, res.KmerCap  )
	res.Buffer    = make(KmerArr, 0, res.BufferCap)
 
    var list []uint64 = []uint64{1,2,3,4,5,6,7,8,9,10}
 
    println("before")
    res.Print()
    println()
    for _,j := range list {
        println("adding", j)
        res.Add(j)
        res.Print()
        println()
    }
    res.Close()
}