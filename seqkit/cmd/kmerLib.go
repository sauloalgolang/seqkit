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
	for i,j := range *this {
		log.Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
	}
}

func (this *KmerArr) Add(kmer uint64) {
	//log.Debugf("KmerArr    :: Add %3d %p", kmer, (*this))
	(*this) = append((*this), KmerUnit{kmer, 1})
	//log.Debugf("KmerArr    :: Add %d %p", kmer, (*this))
}

func (this *KmerArr) Clear() {
	log.Debugf("KmerArr    :: Clear %p LEN %d CAP %d", (*this), len((*this)), cap((*this)))
	(*this) = (*this)[:0]
	log.Debugf("KmerArr    :: Clear %p LEN %d CAP %d", (*this), len((*this)), cap((*this)))
}

func (this *KmerArr) isEqual(that *KmerArr) (bool, string) {
	log.Debugf("KmerArr    :: isEqual", (*this), len((*this)), cap((*this)), (*that), len((*that)), cap((*that)))

	if len(*this) != len(*that) {
		log.Debugf("KmerArr    :: isEqual :: Sizes differ")
		return false, "Sizes differ"
	}
	
	for i,j := range (*this) {
		if j.Kmer != (*that)[i].Kmer {
			log.Debugf("KmerArr    :: isEqual :: Kmer out of order")
			return false, "Kmer out of order"
		}
		if j.Count != (*that)[i].Count {
			log.Debugf("KmerArr    :: isEqual :: Kmer count differ")
			return false, "Kmer count differ"
		}
	}	

	log.Debugf("KmerArr    :: isEqual :: OK")
	return true, "OK"
}







func sortSlice(this *KmerArr) {
	sortSliceOffset(this, 0)
}

func sortSliceOffset(this *KmerArr, offset int) {
	sort.Slice((*this)[offset:], func(i, j int) bool {
		return (*this)[offset+i].Kmer < (*this)[offset+j].Kmer
	})	
}


func sumInt8( a, b uint8 ) uint8 {
	if a < 254 {
		if uint64(a) + uint64(b) > 254 {
			return 254
		} else {
			return a + b
		}
	} else {
		return 254
	}
}


func mergeSortedSliceValues(this *KmerArr) int {
	lasti := 0
	for i,_ := range (*this) {
		if i != lasti {
			if (*this)[i].Kmer == (*this)[lasti].Kmer {
				(*this)[lasti].Count = sumInt8( (*this)[lasti].Count, (*this)[i].Count )
			} else {
				lasti++
				(*this)[lasti], (*this)[i] = (*this)[i], (*this)[lasti]
			}
		}
	}
	return lasti
}






type KmerHolder struct {
	KmerSize     uint64
	KmerLen       int
	BufferLen     int
	LastBufferLen int
	KmerCap       int
	BufferCap     int
	Kmer         KmerArr
	Buffer       KmerArr
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

func (this *KmerHolder) Sort2() {
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field

	if len(this.Buffer) < ( 4 * (cap(this.Buffer) / 5)) {
		return
	}
	
	log.Infof("KmerArr    :: Sort %p Len %d Cap %d Prop %f LastBufferLen %d", this.Buffer, len(this.Buffer), cap(this.Buffer), float64(len(this.Buffer)) / float64(cap(this.Buffer)) * 100.0, this.LastBufferLen)
	
	//this.Buffer.Print()

	if this.LastBufferLen == 0 {
		//log.Infof("KmerArr    :: Sort :: All")
		sortSlice(&this.Buffer)
	} else {
		//log.Infof("KmerArr    :: Sort :: Part")
		//log.Infof("\n",this.Buffer[this.LastBufferLen:])

		sortSliceOffset(&this.Buffer, this.LastBufferLen)
		sortSlice(&this.Buffer)
	}
	
	//this.Buffer.Print()

	count := len(this.Buffer)
	lasti := mergeSortedSliceValues(&this.Buffer)
	
	if lasti != len(this.Buffer) - 1 {
		this.Buffer = this.Buffer[:lasti+1]
	}
	
	sumCount := 0
	for i,_ := range this.Buffer {
		sumCount += int(this.Buffer[i].Count)
	}
	
	log.Debugf("KmerArr    :: Sort :: Count %d", count   )
	log.Debugf("KmerArr    :: Sort :: Sum   %d", sumCount)
	log.Debugf("KmerArr    :: Sort :: Len   %d", len(this.Buffer))
	log.Debugf("KmerArr    :: Sort :: Cap   %d", cap(this.Buffer))
	log.Debugf("KmerArr    :: Sort :: %p", this.Buffer)

	if len(this.Buffer) >= ( 4 * (cap(this.Buffer) / 5)) {
		log.Debugf("KmerArr    :: Sort :: Extend :: %p", this.Buffer)
		newCap := (cap(this.Buffer) / 4 * 6)
		log.Debugf("KmerArr    :: Sort :: Extend :: new cap %d", newCap)
		
		t := make(KmerArr, len(this.Buffer), newCap)
		copy(t, this.Buffer)
		this.Buffer = t
		
		log.Infof("KmerArr    :: Sort :: Extend :: Len %d\n", len(this.Buffer))
		log.Infof("KmerArr    :: Sort :: Extend :: Cap %d\n", cap(this.Buffer))
		log.Infof("KmerArr    :: Sort :: Extend :: %p\n", this.Buffer)
		
		//log.Debugf("KmerArr    :: Sort :: Extend :: Len %d: ", len(this.Buffer))
		//log.Debugf("KmerArr    :: Sort :: Extend :: Cap %d: ", cap(this.Buffer))
		//log.Debugf("KmerArr    :: Sort :: Extend :: %p", this.Buffer)
	}
	
	//this.Buffer.Print()

	this.KmerLen = len(this.Kmer)
	this.BufferLen = len(this.Buffer)
	this.LastBufferLen = len(this.Buffer)
}

func (this *KmerHolder) Merge(arr_dst *KmerArr, arr_src *KmerArr) {
	log.Debugf("KmerArr    :: Merge :: Begin :: THIS: %p LEN %d CAP %d", (*arr_dst), len((*arr_dst)), cap((*arr_dst)))
	log.Debugf("KmerArr    :: Merge :: Begin :: THAT: %p LEN %d CAP %d", (*arr_src), len((*arr_src)), cap((*arr_src)))

	lenDst   := len((*arr_dst))
	lenSrc   := len((*arr_src))
	lenAll   := lenDst + lenSrc

	indexDst := 0
	indexSrc := 0

	if lenDst == 0 {
		log.Debugf("KmerArr    :: Merge :: Copy :: THIS: %p LEN %d CAP %d", (*arr_dst), len((*arr_dst)), cap((*arr_dst)))
		(*arr_dst) = make(KmerArr, len((*arr_src)), len((*arr_src)))
		copy((*arr_dst), (*arr_src))
		
	} else {
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: THIS: %p LEN %d CAP %d", (*arr_dst), len((*arr_dst)), cap((*arr_dst)))

		(*arr_dst) = append((*arr_dst), make(KmerArr, lenAll, lenAll)...)
		
		var dstKmer  *KmerUnit
		var srcKmer  *KmerUnit
		
		var dstKmerK *uint64
		var srcKmerK *uint64
		
		var dstKmerC *uint8
		var srcKmerC *uint8
				
		for {
			log.Debugf("")
			log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexDst : %d",   indexDst )
			log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc : %d",   indexSrc )
			if indexDst == lenDst { // no more this
				log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexDst == lenDst" )
				if indexSrc == lenSrc { // no more that. should be fully sorted
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc == lenSrc. breaking" )
					break
				} else { // still that. append
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc != lenSrc. appending" )
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc != lenSrc. appending :: indexDst: ", indexDst )
					for ;indexSrc<lenSrc; indexSrc++ {
						(*arr_dst)[indexDst] = (*arr_src)[indexSrc]
						indexDst++
						log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc != lenSrc. appending :: indexDst: %d indexSrc: %d", indexDst, indexSrc )
					}
					break
				}
			} else { // still has this
				log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexDst != lenDst" )
				if indexSrc == lenSrc { // no more that. should be fully sorted
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc == lenSrc. breaking" )
					break
				
				} else { // still this and still that. keep sorting
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc != lenSrc. merging" )
					
					dstKmer  = &(*arr_dst)[indexDst]
					srcKmer  = &(*arr_src)[indexSrc]
					
					dstKmerK = &dstKmer.Kmer
					srcKmerK = &srcKmer.Kmer
					
					dstKmerC = &dstKmer.Count
					srcKmerC = &srcKmer.Count
					
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: dstKmerK : %d", (*dstKmerK))
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: srcKmerK : %d", (*srcKmerK))
				
					if *dstKmerK == *srcKmerK {
						*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
						
						indexDst++
						indexSrc++
						
					} else if *dstKmerK < *srcKmerK {
						indexDst++
						
					} else if *dstKmerK > *srcKmerK {
						(*arr_dst)[indexDst], (*arr_src)[indexSrc] = (*arr_src)[indexSrc], (*arr_dst)[indexDst]

						sortSliceOffset(arr_src, indexSrc)
												
						//indexSrcK := indexSrc
						//for {
						//	if indexSrcK < lenSrc-1 && (*arr_src)[indexSrcK].Kmer > (*arr_src)[indexSrcK+1].Kmer {
						//		(*arr_src)[indexSrcK], (*arr_src)[indexSrcK+1] = (*arr_src)[indexSrcK+1], (*arr_src)[indexSrcK]
						//		indexSrcK++
						//	} else {
						//		break
						//	}
						//}
						//indexDst++
					}
				}
			}
		}
	
		log.Debugf( "this" )
		arr_dst.Print()

		log.Debugf( "that" )
		arr_src.Print()

		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: lenDst   : %d",   lenDst)
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexDst : %d", indexDst)
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc : %d", indexSrc)
		
		boundaryThis := indexDst
		if indexDst < lenDst {
			boundaryThis = lenDst
		}
	
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Clear :: THIS: %p LEN %d CAP %d", (*arr_dst), len((*arr_dst)), cap((*arr_dst)))
		(*arr_dst) = (*arr_dst)[:boundaryThis]
		log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Clear :: THIS: %p LEN %d CAP %d", (*arr_dst), len((*arr_dst)), cap((*arr_dst)))
	}
	
	(*arr_src).Clear()
	
	log.Debugf("KmerArr    :: Merge :: Finish :: THIS: %p LEN %d CAP %d", (*arr_dst), len((*arr_dst)), cap((*arr_dst)))
	log.Debugf("KmerArr    :: Merge :: Finish :: THAT: %p LEN %d CAP %d", (*arr_src), len((*arr_src)), cap((*arr_src)))
}






func (this *KmerHolder) Sort() {
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field

	if len(this.Buffer) < ( 4 * (cap(this.Buffer) / 5)) {
		return
	}
	
	log.Infof("KmerArr    :: Sort %p Len %d Cap %d Prop %f LastBufferLen %d", this.Buffer, len(this.Buffer), cap(this.Buffer), float64(len(this.Buffer)) / float64(cap(this.Buffer)) * 100.0, this.LastBufferLen)
	
	//this.Buffer.Print()
	lasti := 0
	count := len(this.Buffer)
	sumCountBefore := 0

	if this.LastBufferLen == 0 {
		//log.Infof("KmerArr    :: Sort :: All")

		for i,_ := range this.Buffer {
			sumCountBefore += int(this.Buffer[i].Count)
		}

		sort.Slice(this.Buffer[:], func(i, j int) bool {
			return this.Buffer[i].Kmer < this.Buffer[j].Kmer
		})

		for i,_ := range this.Buffer {
			if i != lasti {
				if this.Buffer[i].Kmer == this.Buffer[lasti].Kmer {
					if this.Buffer[lasti].Count < 254 {
						if uint64(this.Buffer[lasti].Count) + uint64(this.Buffer[i].Count) > 254 {
							this.Buffer[lasti].Count  = 254
						} else {
							this.Buffer[lasti].Count += this.Buffer[i].Count
						}
					}
				} else {
					lasti++
					this.Buffer[lasti], this.Buffer[i] = this.Buffer[i], this.Buffer[lasti]
				}
			}
		}
	} else {
		for i,_ := range this.Buffer[:this.LastBufferLen] {
			sumCountBefore += int(this.Buffer[i].Count)
		}

		log.Infof("KmerArr    :: Sort :: Part :: Before")
		this.Buffer.Print()

		sort.Slice(this.Buffer[this.LastBufferLen:], func(i, j int) bool {
			return this.Buffer[this.LastBufferLen+i].Kmer < this.Buffer[this.LastBufferLen+j].Kmer
		})
		
		log.Infof("KmerArr    :: Sort :: Part :: During")
		this.Buffer.Print()
		
		indexDst := 0
		indexSrc := this.LastBufferLen
	
		lenDst   := this.LastBufferLen
		lenSrc   := len(this.Buffer)
		
		var dstKmer  *KmerUnit
		var srcKmer  *KmerUnit
		
		var dstKmerK *uint64
		var srcKmerK *uint64
		
		var dstKmerC *uint8
		var srcKmerC *uint8
				
		for {
			if indexDst == lenDst { // no more this
				log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexDst (%d) == lenDst (%d)", indexDst, lenDst )
				if indexSrc == lenSrc { // no more that. should be fully sorted
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%d) == lenSrc (%d). breaking", indexSrc, lenSrc )
					break
				} else { // still that. append
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%d) != lenSrc (%d). appending", indexSrc, lenSrc )
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%d) != lenSrc (%d). appending :: indexDst: %d", indexSrc, lenSrc, indexDst )
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%d) != lenSrc (%d). appending :: indexDst: %d (%d=%d) indexSrc: %d (%d=%d)", indexSrc, lenSrc, indexDst, this.Buffer[indexDst].Kmer, this.Buffer[indexDst].Count, indexSrc, this.Buffer[indexSrc].Kmer, this.Buffer[indexSrc].Count )
					for ;indexSrc<lenSrc; indexSrc++ {
						if this.Buffer[indexDst].Kmer == this.Buffer[indexSrc].Kmer {
							if this.Buffer[indexDst].Count < 254 {
								if uint64(this.Buffer[indexDst].Count) + uint64(this.Buffer[indexSrc].Count) > 254 {
									this.Buffer[indexDst].Count  = 254
									log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Fulled")
								} else {
									this.Buffer[indexDst].Count += this.Buffer[indexSrc].Count
									log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Summed")
								}
							} else {
								log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Full")
							}
						} else {
							this.Buffer[indexDst] = this.Buffer[indexSrc]
							indexDst++
						}
					}
					break
				}
			} else { // still has this
				log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexDst (%d) != lenDst (%d)", indexDst, lenDst  )
				if indexSrc == lenSrc { // no more that. should be fully sorted
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%d) == lenSrc (%d). breaking", indexSrc, lenSrc )
					break
				
				} else { // still this and still that. keep sorting
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%d) != lenSrc (%d). merging", indexSrc, lenSrc )
					
					dstKmer  = &this.Buffer[indexDst]
					srcKmer  = &this.Buffer[indexSrc]
					
					dstKmerK = &dstKmer.Kmer
					srcKmerK = &srcKmer.Kmer
					
					dstKmerC = &dstKmer.Count
					srcKmerC = &srcKmer.Count
					
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: dstKmerK : %d", (*dstKmerK))
					log.Debugf("KmerArr    :: Merge :: Merge & Sort :: srcKmerK : %d", (*srcKmerK))
				
					if *dstKmerK == *srcKmerK {
						log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Adding")
						if *dstKmerC < 254 {
							if uint64(*dstKmerC) + uint64(*srcKmerC) > 254 {
								*dstKmerC  = 254
								log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Fulled")
							} else {
								*dstKmerC += *srcKmerC
								log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Summed")
							}
						} else {
							log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Full")
						}
						
						//indexDst++
						indexSrc++
						
					} else if *dstKmerK < *srcKmerK {
						log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Next Dst")
						indexDst++
						
					} else if *dstKmerK > *srcKmerK {
						log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping")

						this.Buffer[indexDst], this.Buffer[indexSrc] = this.Buffer[indexSrc], this.Buffer[indexDst]
						
						sort.Slice(this.Buffer[indexSrc:], func(i, j int) bool {
							return this.Buffer[indexSrc+i].Kmer < this.Buffer[indexSrc+j].Kmer
						})
						
						//indexSrcK := indexSrc
						//for {
						//	if indexSrcK < lenSrc-1 && (*arr_src)[indexSrcK].Kmer > (*arr_src)[indexSrcK+1].Kmer {
						//		(*arr_src)[indexSrcK], (*arr_src)[indexSrcK+1] = (*arr_src)[indexSrcK+1], (*arr_src)[indexSrcK]
						//		indexSrcK++
						//	} else {
						//		break
						//	}
						//}
						//indexDst++
					}
				}
			}
		}

		log.Infof("KmerArr    :: Sort :: Part :: After :: indexDst: (%d)", indexDst)
		this.Buffer.Print()

		lasti = indexDst
	}

	if lasti != len(this.Buffer) - 1 {
		this.Buffer = this.Buffer[:lasti+1]
	}
	
	sumCount := 0
	for i,_ := range this.Buffer {
		sumCount += int(this.Buffer[i].Count)
	}
	
	log.Debugf("KmerArr    :: Sort :: Count %d", count           )
	log.Debugf("KmerArr    :: Sort :: Sum B %d", sumCountBefore  )
	log.Debugf("KmerArr    :: Sort :: Sum A %d", sumCount        )
	log.Debugf("KmerArr    :: Sort :: Len   %d", len(this.Buffer))
	log.Debugf("KmerArr    :: Sort :: Cap   %d", cap(this.Buffer))
	log.Debugf("KmerArr    :: Sort :: %p", this.Buffer)

	log.Infof("KmerArr    :: Sort :: Final")
	this.Buffer.Print()


	if len(this.Buffer) >= ( 4 * (cap(this.Buffer) / 5)) {
		log.Debugf("KmerArr    :: Sort :: Extend :: %p", this.Buffer)
		newCap := (cap(this.Buffer) / 4 * 6)
		log.Debugf("KmerArr    :: Sort :: Extend :: new cap %d", newCap)
		
		t := make(KmerArr, len(this.Buffer), newCap)
		copy(t, this.Buffer)
		this.Buffer = t
		
		log.Infof("KmerArr    :: Sort :: Extend :: Len %d\n", len(this.Buffer))
		log.Infof("KmerArr    :: Sort :: Extend :: Cap %d\n", cap(this.Buffer))
		log.Infof("KmerArr    :: Sort :: Extend :: %p\n", this.Buffer)
		
		//log.Debugf("KmerArr    :: Sort :: Extend :: Len %d: ", len(this.Buffer))
		//log.Debugf("KmerArr    :: Sort :: Extend :: Cap %d: ", cap(this.Buffer))
		//log.Debugf("KmerArr    :: Sort :: Extend :: %p", this.Buffer)
	}
	
	//this.Buffer.Print()

	this.KmerLen = len(this.Kmer)
	this.BufferLen = len(this.Buffer)
	this.LastBufferLen = len(this.Buffer)
}











func (this *KmerHolder) Add(kmer uint64) {
	//log.Debugf("KmerHolder :: Add %3d %p", kmer, this.Buffer)
	//	this.Sort()
	//	log.Debugf("KmerHolder :: Add SORTED %3d %p", kmer, this.Buffer)
	//}
	
	//if this.BufferLen == this.BufferCap {
	//	this.Sort()
	//	log.Debugf("KmerHolder :: Add SORTED %3d %p", kmer, this.Buffer)
	//}

	this.Sort()
	this.Buffer.Add(kmer)
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
	max_kmer_size := (2 << (kmerSize*2)) / 2
	
	buffer_cap := 1000000
	
	if buffer_cap > max_kmer_size / 10 {
		buffer_cap = max_kmer_size / 10
	}
	
	//if buffer_cap < 10000 {
	//	buffer_cap = 10000
	//}
	
	kmer_cap := max_kmer_size / 100
	if kmer_cap < buffer_cap {
		kmer_cap = buffer_cap
	}
	
	
	var k KmerHolder = KmerHolder{}
	k.KmerSize       = kmerSize
	k.KmerLen        = 0
	k.BufferLen      = 0
	k.LastBufferLen  = 0
	k.KmerCap        = kmer_cap
	k.BufferCap      = buffer_cap
	k.Kmer           = make(KmerArr, 0, k.KmerCap  )
	k.Buffer         = make(KmerArr, 0, k.BufferCap)

	log.Infof(p.Sprintf( "kmer size   %12d\n", k.KmerSize    ))
	log.Infof(p.Sprintf( "max db size %12d\n", max_kmer_size ))
	log.Infof(p.Sprintf( "kmer cap    %12d\n", k.KmerCap     ))
	log.Infof(p.Sprintf( "buffer cap  %12d\n", k.BufferCap   ))
	
	return &k
}
