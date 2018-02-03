package cmd

import (
	"sort"
	"golang.org/x/text/message"
)

var p = message.NewPrinter(message.MatchLanguage("en"))



type KmerUnit struct {
	Kmer  uint64
	Count uint8
}

type KmerArr []KmerUnit

func (this *KmerArr) Print() {
	//p.Printf( "kmerSize  %12d\n", kmerSize    )
	for i,j := range *this {
		p.Printf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count )
	}
}

func (this *KmerArr) Sort() {
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field
	sort.Slice((*this)[:], func(i, j int) bool {
		return (*this)[i].Kmer < (*this)[j].Kmer
	})
}

func (this *KmerArr) Merge(that *KmerArr) {
	lenThis   :=           len((*this))
	lenThat   := lenThis + len((*that))

	indexThis := 0
	indexThat := lenThis

	(*this)    = append((*this), (*that)...)
	
	var thisKmer  *KmerUnit
	var thatKmer  *KmerUnit
	
	var thisKmerK *uint64
	var thatKmerK *uint64
	
	var thisKmerC *uint8
	var thatKmerC *uint8
	
	for {
		if indexThat == lenThat-1 {
			break
		}
		
		thisKmer  = &(*this)[indexThis]
		thatKmer  = &(*that)[indexThat]
		
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
			(*this)[indexThis], (*this)[indexThat] = (*that)[indexThat], (*that)[indexThis]
			indexThat++
		}
	}
	
	(*this) = (*this)[:indexThis]
}

func (this *KmerArr) Add(kmer uint64) {
	println("KmerArr    :: Add", kmer, (*this))
	(*this) = append((*this), KmerUnit{kmer, 1})
	println("KmerArr    :: Add", kmer, (*this))
}

func (this *KmerArr) Clean() {
	
}




type KmerHolder struct {
	KmerSize    uint64
	KmerLen      int
	BufferLen    int
	KmerCap      int
	BufferCap    int
	Kmer        KmerArr
	Buffer      KmerArr
}

func (this *KmerHolder) Print() {	
	p.Printf( "kmerSize     %12d\n", this.KmerSize  )
	p.Printf( "KmerLen      %12d\n", this.KmerLen   )
	p.Printf( "BufferLen    %12d\n", this.BufferLen )
	p.Printf( "KmerCap      %12d\n", this.KmerCap   )
	p.Printf( "BufferCap    %12d\n", this.BufferCap )

	p.Printf( "Kmer         %12d CAP %12d\n", len(this.Kmer  ), cap(this.Kmer  ) )
	this.Kmer.Print()
	
	p.Printf( "Buffer       %12d CAP %12d\n", len(this.Buffer), cap(this.Buffer) )
	this.Buffer.Print()
}

func (this *KmerHolder) Sort() {
	println("KmerHolder :: Sort")
	this.Buffer.Sort()
	this.Kmer.Merge(&this.Buffer)
	this.Buffer.Clean()
	this.KmerLen = len(this.Kmer)
	this.BufferLen = len(this.Buffer)
}

func (this *KmerHolder) Add(kmer uint64) {
	println("KmerHolder :: Add", kmer, this.Buffer)
	if this.BufferLen == this.BufferCap {
		this.Sort()
	}
	println("KmerHolder :: Add", kmer, this.Buffer)
	this.Buffer.Add(kmer)
	println("KmerHolder :: Add", kmer, this.Buffer)
	this.BufferLen = len(this.Buffer)
}

func (this *KmerHolder) Get(i int) KmerUnit {
	this.Close()
	
	return this.Kmer[i]
}

func (this *KmerHolder) Close() {
	this.Sort()
}


func NewKmerHolder(kmerSize uint64) *KmerHolder {
	var k KmerHolder = KmerHolder{}
	k.KmerSize       = kmerSize
	k.KmerLen        =        0
	k.BufferLen      =        0
	k.KmerCap        =     1000
	k.BufferCap      =     1000
	k.Kmer           = make(KmerArr, 0, k.KmerCap  )
	k.Buffer         = make(KmerArr, 0, k.BufferCap)
	return &k
}
