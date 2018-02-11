package cmd

import (
	"runtime"
	"sort"
	"golang.org/x/text/message"
	"github.com/shenwei356/go-logging"
)

//https://stackoverflow.com/questions/6878590/the-maximum-value-for-an-int-type-in-go
const MaxUint = ^uint64(0) 
const MinUint = 0 
const MaxInt = int64(MaxUint >> 1) 
const MinInt = -MaxInt - 1

var p = message.NewPrinter(message.MatchLanguage("en"))


type KmerUnit struct {
	Kmer  uint64
	Count uint8
}

type KmerArr []KmerUnit

func (this *KmerArr) Print() {
	lvl, _ := logging.LogLevel("DEBUG")
	(*this).PrintLevel(lvl)
}

func (this *KmerArr) PrintLevel(lvl logging.Level) {
	if logging.GetLevel("seqkit") >= lvl {
		for i,j := range *this {
			//log.Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
			p.Printf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count )
		}
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

func moveDownWhileSmall(this *KmerArr, offset int) {
	for i,_ := range ((*this)[offset:]) {
		//println("i",i+offset)
		if i < len((*this))-offset-1 {
			//println("i",i+offset,"<len(",len((*this)),")-offset(",offset,")")
			if (*this)[offset+i].Kmer > (*this)[offset+i+1].Kmer {
				//println("i",(*this)[offset+i].Kmer,">",(*this)[offset+i+1].Kmer," :: swapping")
				(*this)[offset+i], (*this)[offset+i+1] = (*this)[offset+i+1], (*this)[offset+i]
			} else {
				//println("i",(*this)[offset+i].Kmer,"<=",(*this)[offset+i+1].Kmer," :: breaking")
				break
			}
		} else {
			//println("i",i+offset,">=len(",len((*this)),")-offset(",offset,")")
			break
		}
	}
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


func (this *KmerHolder) Sort() {
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field

	if len(this.Buffer) < ( 9 * (cap(this.Buffer) / 10)) {
		return
	}
	
	lvlD, _ := logging.LogLevel("DEBUG")
	lvlI, _ := logging.LogLevel("INFO" )
	
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Before :: %p Len %3d Cap %3d Prop %6.2f LastBufferLen %3d", this.Buffer, len(this.Buffer), cap(this.Buffer), float64(len(this.Buffer)) / float64(cap(this.Buffer)) * 100.0, this.LastBufferLen)
	
	lasti           := 0
	//lenBufferBefore := len(this.Buffer)
	sumCountBefore  := 0

	var dstKmer  *KmerUnit
	var srcKmer  *KmerUnit
	
	var dstKmerK *uint64
	var srcKmerK *uint64
	
	var dstKmerC *uint8
	var srcKmerC *uint8

	var minK uint64 = MaxUint
	var maxK uint64 = 0
	
	if this.LastBufferLen == 0 { // first adding
		//log.Infof("KmerArr    :: Sort :: All")

		sortSlice(&this.Buffer) // sort buffer

		for i,_ := range this.Buffer {
			dstKmer  = &this.Buffer[lasti]
			srcKmer  = &this.Buffer[i]

			dstKmerK = &dstKmer.Kmer
			srcKmerK = &srcKmer.Kmer

			dstKmerC = &dstKmer.Count
			srcKmerC = &srcKmer.Count

			if *srcKmerK < *dstKmerK {
				if *srcKmerK < minK {
					minK = *srcKmerK
				}
				if *dstKmerK > maxK {
					maxK = *dstKmerK
				}
			} else {
				if *dstKmerK < minK {
					minK = *dstKmerK
				}
				if *srcKmerK > maxK {
					maxK = *srcKmerK
				}
			}
			
			sumCountBefore += int(*srcKmerC)

			if i != lasti {
				if *dstKmerK == *srcKmerK { // same key
					*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
				} else { // different key
					lasti++ // next last
					// swap next and continue loop
					this.Buffer[lasti], this.Buffer[i] = this.Buffer[i], this.Buffer[lasti]
				}
			}
		}
	} else {
		for i,_ := range this.Buffer { //sum buffer
			sumCountBefore += int(this.Buffer[i].Count)
		}

		indexDst := 0
		indexSrc := this.LastBufferLen
	
		lenDst   := this.LastBufferLen
		lenSrc   := len(this.Buffer)

		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Before")
		this.Buffer.Print()

		sortSliceOffset(&this.Buffer, lenDst)
		
		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: During")
		this.Buffer.Print()
		
		for {
			if indexSrc == lenSrc { // no more buffer. stop
				//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%03d) == lenSrc (%03d). breaking", indexSrc, lenSrc )
				break
			} else { // still has buffer
				//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%03d) != lenSrc (%03d). merging", indexSrc, lenSrc )
				
				dstKmer  = &this.Buffer[indexDst]
				srcKmer  = &this.Buffer[indexSrc]
				
				dstKmerK = &dstKmer.Kmer
				srcKmerK = &srcKmer.Kmer
				
				dstKmerC = &dstKmer.Count
				srcKmerC = &srcKmer.Count

				if *srcKmerK < *dstKmerK {
					if *srcKmerK < minK {
						minK = *srcKmerK
					}
					if *dstKmerK > maxK {
						maxK = *dstKmerK
					}
				} else {
					if *dstKmerK < minK {
						minK = *dstKmerK
					}
					if *srcKmerK > maxK {
						maxK = *srcKmerK
					}
				}
				
				//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: dstIndex % 3d srcIndex % 3d",  indexDst,  indexSrc)
				//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: dstKmerK %03d srcKmerK %03d", *dstKmerK, *srcKmerK)

				if *dstKmerK == *srcKmerK { //same kmer
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Adding")

					//sum
					*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
					
					//next src
					indexSrc++
					
				} else if *dstKmerK < *srcKmerK { //db < buffer
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order")
					
					if indexDst >= lenDst {
						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Swapping :: indexDst %03d LastBufferLen %03d", indexDst, this.LastBufferLen)
						this.Buffer.Print()
						this.Buffer[indexDst], this.Buffer[indexSrc] = this.Buffer[indexSrc], this.Buffer[indexDst]

						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d", indexSrc)
						this.Buffer.Print()
						
						moveDownWhileSmall(&this.Buffer, indexSrc)
						
						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
						this.Buffer.Print()
						
						lenDst = indexDst + 1
						indexSrc++
					} else {
						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Next Dst")
						
						this.Buffer.Print()
							
						//next db
						indexDst++
					}
				} else if *dstKmerK > *srcKmerK { //db > buffer. worst case scnenario
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping")

					this.Buffer.Print()

					//swapping values
					this.Buffer[indexDst], this.Buffer[indexSrc] = this.Buffer[indexSrc], this.Buffer[indexDst]
					
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d", indexSrc)
					this.Buffer.Print()
					
					moveDownWhileSmall(&this.Buffer, indexSrc)
					
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
					this.Buffer.Print()
				}
			}
		}

		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: After :: indexDst: (%03d)", indexDst)
		this.Buffer.Print()

		if indexDst < lenDst {
			lasti = lenDst - 1
		} else {
			lasti = indexDst
		}
	}

	if lasti != len(this.Buffer) - 1 {
		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Trimming :: last I %3d Len Buffer %3d", lasti, len(this.Buffer))
		this.Buffer = this.Buffer[:lasti+1]
	}
	
	sumCountAfter := 0
	for i,_ := range this.Buffer {
		sumCountAfter += int(this.Buffer[i].Count)
	}
	
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Len Buffer Before %3d", lenBufferBefore   )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Last Buffer Len   %3d", this.LastBufferLen)
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Last I            %3d", lasti             )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Sum B             %3d", sumCountBefore    )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Sum A             %3d", sumCountAfter     )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Len               %3d", len(this.Buffer)  )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Cap               %3d", cap(this.Buffer)  )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Min K             %3d", minK              )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Max K             %3d", maxK              )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: %p", this.Buffer)

	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Final")

	this.Buffer.PrintLevel(lvlD)

	if sumCountBefore != sumCountAfter {
		//this.Buffer.PrintLevel(lvlI)
		//log.Debugf("sum differs")
	}
	
	if len(this.Buffer) >= ( 4 * (cap(this.Buffer) / 5)) {
		newCap := (cap(this.Buffer) / 4 * 6)
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: Len     %d", len(this.Buffer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: Cap     %d", cap(this.Buffer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: New Cap %d", newCap)
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: Address %p", this.Buffer)
		
		t := make(KmerArr, len(this.Buffer), newCap)
		copy(t, this.Buffer)
		this.Buffer = t
		
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: Len     %d", len(this.Buffer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: Cap     %d", cap(this.Buffer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: New Cap %d", newCap)
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: Address %p", this.Buffer)
		
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Running GC")
		runtime.GC()
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: GC Run")
	}
	
	//this.Buffer.Print()

	if len(this.Buffer) < this.LastBufferLen {
		this.Buffer.PrintLevel(lvlI)
		log.Panicf("BUFFER REDUCED SIZE")
	}
	
	this.KmerLen = len(this.Kmer)
	this.BufferLen = len(this.Buffer)
	this.LastBufferLen = len(this.Buffer)

	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: After  :: %p Len %3d Cap %3d Prop %6.2f LastBufferLen %3d", this.Buffer, len(this.Buffer), cap(this.Buffer), float64(len(this.Buffer)) / float64(cap(this.Buffer)) * 100.0, this.LastBufferLen)
	//this.Buffer.PrintLevel(lvlI)
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
