package cmd

// go test -v

import (
        "testing"
        "github.com/shenwei356/go-logging"
)

func TestSort(t *testing.T) {
    t.Run("1,1,2,3,4"            , testList(2, 3, 2, []uint64{1,1,2,3,4}            , KmerArr{{1,2},{2,1},{3,1},{4,1}}))
    t.Run("1,2,3,4,5"            , testList(2, 3, 2, []uint64{1,2,3,4,5}            , KmerArr{{1,1},{2,1},{3,1},{4,1},{5,1}}))
    t.Run("1,2,3,4,5,6,7,8,9,10" , testList(2, 3, 2, []uint64{1,2,3,4,5,6,7,8,9,10} , KmerArr{{1,1},{2,1},{3,1},{4,1},{5,1},{6,1},{7,1},{8,1},{9,1},{10,1}}))
    t.Run("1,2,3,4,5,1,2,3,4,5"  , testList(2, 3, 2, []uint64{1,2,3,4,5,1,2,3,4,5}  , KmerArr{{1,2},{2,2},{3,2},{4,2},{5,2}}))
    t.Run("1,2,3,4,5,1,2,3,4,5,6", testList(2, 3, 2, []uint64{1,2,3,4,5,1,2,3,4,5,6}, KmerArr{{1,2},{2,2},{3,2},{4,2},{5,2},{6,1}}))
    t.Run("1,6,2,5,3,3"          , testList(2, 3, 2, []uint64{1,6,2,5,3,3}          , KmerArr{{1,1},{2,1},{3,2},{5,1},{6,1}}))
    t.Run("1,6,5,2,3,3,0"        , testList(2, 3, 2, []uint64{1,6,5,2,3,3,0}        , KmerArr{{0,1},{1,1},{2,1},{3,2},{5,1},{6,1}}))
}

func testList(kmerSize uint64, KmerCap int, BufferCap int, list []uint64, check KmerArr) func(*testing.T) {
    return func (t *testing.T) {
        logging.SetLevel(logging.DEBUG, "seqkit")
        
        var res       = NewKmerHolder(kmerSize)
        res.KmerSize  = kmerSize
        res.KmerCap   = KmerCap
        res.BufferCap = BufferCap
        res.Kmer      = make(KmerArr, 0, res.KmerCap  )
        res.Buffer    = make(KmerArr, 0, res.BufferCap)
     
        log.Debug("testing", list)
        log.Debug("before")
        res.Print()
        log.Debug("")
        for _,j := range list {
            log.Debug("adding", j)
            res.Add(j)
            res.Print()
            log.Debug("")
        }
        res.Close()
        res.Print()
        
        status, msg := res.Kmer.isEqual(&check)
        
        if status {
            t.Log("For", list, "- OK")
        } else {
            t.Error(
                    "For"     , list    ,
                    "Expected", check   ,
                    "Got"     , res.Kmer,
                    "Error"   , msg)
        }
        
        t.Log("==========================")
    }
}