package cmd

import (
	"sort"
	"golang.org/x/text/message"
	//"github.com/op/go-logging"
	//"github.com/shenwei356/go-logging"
)

//https://github.com/op/go-logging

//var log = logging.MustGetLogger("Kmer")

//var format = logging.MustStringFormatter(
//	`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
//)

var p = message.NewPrinter(message.MatchLanguage("en"))


type KmerUnit struct {
	Kmer  uint64
	Count uint8
}

type KmerArr []KmerUnit

func (this *KmerArr) Print() {
	//log.Debugf(p.Sprintf( "kmerSize  %12d\n", kmerSize    )
	for i,j := range *this {
		log.Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
	}
}

func (this *KmerArr) Sort() {
	log.Debugf("KmerArr    :: Sort", (*this))
	(*this).Print()
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field

	sort.Slice((*this)[:], func(i, j int) bool {
		return (*this)[i].Kmer < (*this)[j].Kmer
	})
	
	log.Debugf("KmerArr    :: Sort", (*this))
	(*this).Print()
}

func (this *KmerArr) Merge(that *KmerArr) {
	log.Debugf("KmerArr    :: Merge :: THIS: ", (*this), len((*this)), cap((*this)))
	log.Debugf("KmerArr    :: Merge :: THAT: ", (*that), len((*that)), cap((*that)))

	lenThis   := len((*this))
	lenThat   := len((*that))
	lenAll    := lenThis + lenThat

	indexThis := 0
	indexThat := 0

	if lenThis == 0 {
		log.Debugf("KmerArr    :: Merge :: Copy :: ", len((*this)), cap((*this)))
		(*this) = make(KmerArr, len((*that)), len((*that)))
		copy((*this), (*that))
		
	} else {
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: ", len((*this)), cap((*this)))

		(*this) = append((*this), make(KmerArr, lenAll, lenAll)...)
		
		var thisKmer  *KmerUnit
		var thatKmer  *KmerUnit
		
		var thisKmerK *uint64
		var thatKmerK *uint64
		
		var thisKmerC *uint8
		var thatKmerC *uint8
				
		for {
			log.Debugf("")
			log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThis : ",   indexThis )
			log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat : ",   indexThat )
			if indexThis == lenThis { // no more this
				log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThis == lenThis" )
				if indexThat == lenThat { // no more that. should be fully sorted
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat == lenThat. breaking" )
					break
				} else { // still that. append
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat != lenThat. appending" )
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat != lenThat. appending :: indexThis: ", indexThis )
					for ;indexThat<lenThat; indexThat++ {
						(*this)[indexThis] = (*that)[indexThat]
						indexThis++
						log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat != lenThat. appending :: indexThis: ", indexThis, " indexThat: ", indexThat )
					}
					break
				}
			} else { // still has this
				log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThis != lenThis" )
				if indexThat == lenThat { // no more that. should be fully sorted
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat == lenThat. breaking" )
					break
				
				} else { // still this and still that. keep sorting
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat != lenThat. merging" )
					
					thisKmer  = &(*this)[indexThis]
					thatKmer  = &(*that)[indexThat]
					
					thisKmerK = &thisKmer.Kmer
					thatKmerK = &thatKmer.Kmer
					
					thisKmerC = &thisKmer.Count
					thatKmerC = &thatKmer.Count
					
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: thisKmerK : ", (*thisKmerK))
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: thatKmerK : ", (*thatKmerK))
				
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
						(*this)[indexThis], (*that)[indexThat] = (*that)[indexThat], (*this)[indexThis]
						indexThatK := indexThat
						for {
							if indexThatK < lenThat-1 && (*that)[indexThatK].Kmer > (*that)[indexThatK+1].Kmer {
								(*that)[indexThatK], (*that)[indexThatK+1] = (*that)[indexThatK+1], (*that)[indexThatK]
								indexThatK++
							} else {
								break
							}
						}
						//indexThis++
					}
					log.Debugf( "this" )
					this.Print()
					log.Debugf( "that" )
					that.Print()
				}
			}
		}
	
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: lenThis   : ",   lenThis)
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThis : ", indexThis)
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexThat : ", indexThat)
		boundaryThis := indexThis
		if indexThis < lenThis {
			boundaryThis = lenThis
		}
	
		(*this) = (*this)[:boundaryThis]
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: ", len((*this)), cap((*this)))
	}
	
	(*that).Clear()
	
	log.Debugf("KmerArr    :: Merge :: THIS: ", (*this), len((*this)), cap((*this)))
	log.Debugf("KmerArr    :: Merge :: THAT: ", (*that), len((*that)), cap((*that)))
}

func (this *KmerArr) Add(kmer uint64) {
	log.Debugf("KmerArr    :: Add", kmer, (*this))
	(*this) = append((*this), KmerUnit{kmer, 1})
	log.Debugf("KmerArr    :: Add", kmer, (*this))
}

func (this *KmerArr) Clear() {
	log.Debugf("KmerArr    :: Clear", (*this), len((*this)), cap((*this)))
	(*this) = (*this)[:0]
	log.Debugf("KmerArr    :: Clear", (*this), len((*this)), cap((*this)))
}

func (this *KmerArr) isEqual(that *KmerArr) (bool, string) {
	if len(*this) != len(*that) {
		return false, "Sizes differ"
	}
	
	for i,j := range (*this) {
		if j.Kmer != (*that)[i].Kmer {
			return false, "Kmer out of order"
		}
		if j.Count != (*that)[i].Count {
			return false, "Kmer count differ"
		}
	}	
	return true, "OK"
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
	log.Debugf(p.Sprintf( "kmerSize     %12d\n", this.KmerSize  ))
	log.Debugf(p.Sprintf( "KmerLen      %12d\n", this.KmerLen   ))
	log.Debugf(p.Sprintf( "BufferLen    %12d\n", this.BufferLen ))
	log.Debugf(p.Sprintf( "KmerCap      %12d\n", this.KmerCap   ))
	log.Debugf(p.Sprintf( "BufferCap    %12d\n", this.BufferCap ))

	log.Debugf(p.Sprintf( "Kmer         %12d CAP %12d\n", len(this.Kmer  ), cap(this.Kmer  ) ))
	this.Kmer.Print()
	
	log.Debugf(p.Sprintf( "Buffer       %12d CAP %12d\n", len(this.Buffer), cap(this.Buffer) ))
	this.Buffer.Print()
}

func (this *KmerHolder) Sort() {
	log.Debugf("KmerHolder :: Sort")
	this.Buffer.Sort()
	this.Kmer.Merge(&this.Buffer)
	this.KmerLen = len(this.Kmer)
	this.BufferLen = len(this.Buffer)
}

func (this *KmerHolder) Add(kmer uint64) {
	log.Debugf("KmerHolder :: Add", kmer, this.Buffer)
	if this.BufferLen == this.BufferCap {
		this.Sort()
	}
	log.Debugf("KmerHolder :: Add", kmer, this.Buffer)
	this.Buffer.Add(kmer)
	log.Debugf("KmerHolder :: Add", kmer, this.Buffer)
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
