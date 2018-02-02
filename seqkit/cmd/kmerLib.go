package cmd

import (
	"sort"
)

type Kmer struct {
	Kmer  uint64
	Count uint8
}

type KmerArr []Kmer

func (this KmerArr) Sort() {
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field
	sort.Slice(this[:], func(i, j int) bool {
		return this[i].Kmer < this[j].Kmer
	})
}

func (this KmerArr) Merge(that KmerArr) {
	lenThis   :=           len(this)
	lenThat   := lenThis + len(that)

	indexThis := 0
	indexThat := lenThis

	this       = append(this, that...)
	
	var thisKmer  *Kmer
	var thatKmer  *Kmer
	
	var thisKmerK *uint64
	var thatKmerK *uint64
	
	var thisKmerC *uint8
	var thatKmerC *uint8
	
	for {
		if indexThat == lenThat-1 {
			break
		}
		
		thisKmer  = &this[indexThis]
		thatKmer  = &this[indexThat]
		
		thisKmerK = &thisKmer.Kmer
		thatKmerK = &thatKmer.Kmer
		
		thisKmerC = &thisKmer.Count
		thatKmerC = &thatKmer.Count
		
		if *thisKmerK == *thatKmerK {
			if *thisKmerC < 254 {
				if uint64(*thisKmerC) + uint64(*thatKmerC) > 254 {
					*thisKmerC  = 254
				} else {
					*thisKmerC += *thatKmerC
				}
			}
			
			indexThis++
			indexThat++
			
		} else if *thisKmerK < *thatKmerK {
			indexThis++
			
		} else if *thisKmerK > *thatKmerK {
			this[indexThis], this[indexThat] = this[indexThat], this[indexThis]
			indexThat++
		}
	}
	
	this = this[:indexThis]
}

func (this KmerArr) Clean() {
	
}




type KmerHolder struct {
	KmerLen   uint64
	Size       int
	ListLen    int
	BufferLen  int
	Kmer      KmerArr
	Buffer    KmerArr
}

func (this KmerHolder) Sort() {
	this.Buffer.Sort()
	this.Kmer.Merge(this.Buffer)
	this.Buffer.Clean()
	this.Size = len(this.Kmer)
}

func (this KmerHolder) Add(kmer uint64) {
	// ADD
	if len(this.Buffer) == this.BufferLen {
		this.Sort()
	}
}

func (this KmerHolder) Get(i int) Kmer {
	this.Close()
	
	return this.Kmer[i]
}

func (this KmerHolder) Close() {
	this.Sort()
}


func NewKmerHolder(kmerLen uint64) *KmerHolder {
	var k KmerHolder = KmerHolder{}
	k.KmerLen   = kmerLen
	k.Size      =       0
	k.ListLen   =    1000
	k.BufferLen =    1000
	k.Kmer      = make(KmerArr, 0, k.ListLen  )
	k.Buffer    = make(KmerArr, 0, k.BufferLen)
	return &k
}
