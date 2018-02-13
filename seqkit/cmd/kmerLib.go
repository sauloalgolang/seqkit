package cmd

import (
	"runtime"
	"sort"
	"golang.org/x/text/message"
	"github.com/shenwei356/go-logging"
)

const min_capacity = 1000000

//https://stackoverflow.com/questions/6878590/the-maximum-value-for-an-int-type-in-go
const MaxUint = ^uint64(0) 
const MinUint = 0 
const MaxInt  = int64(MaxUint >> 1) 
const MinInt  = -MaxInt - 1

var p = message.NewPrinter(message.MatchLanguage("en"))




func sortSlice(this *KmerArr) {
	sortSliceOffset(this, 0)
}

func sortSliceOffset(this *KmerArr, offset int) {
	sort.Slice((*this)[offset:], func(i, j int) bool {
		return (*this)[offset+i].Kmer < (*this)[offset+j].Kmer
	})
}

func moveDownWhileSmall(this *KmerArr, offset int) {
	lt    := len(*this)
	limit := lt-offset-1
	start := 0
	
	for i,_ := range ((*this)[offset:]) {
		//println("i",i+offset)

		if i < limit {
			//println("i",i+offset,"<len(",len((*this)),")-offset(",offset,")")
			start   = offset+i

			if (*this)[start  ].Kmer > (*this)[start+1].Kmer {
				//println("i",(*this)[offset+i].Kmer,">",(*this)[offset+i+1].Kmer," :: swapping")
				(*this)[start  ], (*this)[start+1] = (*this)[start+1], (*this)[start  ]
				//*dstKmer, *srcKmer = *srcKmer, *dstKmer

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

func mergeSortedSliceValues(this *KmerArr) int {
	var dstKmer  *KmerUnit
	var srcKmer  *KmerUnit

	lasti   := 0
	srcKmer  = &(*this)[lasti]
	for i,_ := range (*this) {
		if i != lasti {
			dstKmer  = &(*this)[i]

			if dstKmer.Kmer == (*this)[lasti].Kmer {
				//srcKmer.Count = sumInt8( srcKmer.Count, dstKmer.Count )
				addToInt8(&srcKmer.Count, dstKmer.Count)
			} else {
				lasti++
				(*this)[lasti], (*this)[i] = (*this)[i], (*this)[lasti]
				srcKmer = &(*this)[lasti]
				//*srcKmer, *dstKmer = *dstKmer, *srcKmer
			}
		}
	}
	return lasti
}

func sumInt8( a, b uint8 ) uint8 {
	t := a
	addToInt8( &t, b )
	return t
}

func addToInt8( a *uint8, b uint8 ) {
	if *a < 254 {
		if (254 - *a) >= b {
			*a = 254
		} else {
			*a += b
		}
	} else {
		*a = 254
	}
}





type Converter struct {
	KmerSize  int
	Cleaner  uint64
    CHARS   [4]byte
    chars   [4]byte
	Vals [256][3]uint64
}

func (conv Converter) NumToSeq(kmer uint64) string {
	seq := make([]byte, conv.KmerSize, conv.KmerSize)
	
	for i:=0; i<conv.KmerSize; i++ {
		//seq[i] = 
	}
	
	return string(seq)
}


func NewConverter(kmerSize int) *Converter {
	conv          := Converter{}
	conv.KmerSize  = kmerSize
	conv.Cleaner   = (1 << (uint64(kmerSize)*2)) - 1
	conv.Vals      = [256][3]uint64{}
    conv.CHARS     = [4]byte{'A', 'C', 'G', 'T'}
    conv.chars     = [4]byte{'a', 'c', 'g', 't'}	

	
	for i, _ := range conv.Vals {
		for j, _ := range conv.Vals[i] {
			conv.Vals[i][j] = 0
		}
	}

	for i, b := range conv.CHARS {
		//print( "CHARS i: ", i, " b: ", b, "\n" );
		conv.Vals[uint8(b)][0] =    uint64(i)
		conv.Vals[uint8(b)][1] = (3-uint64(i)) << (2*(uint64(kmerSize)-1))
		conv.Vals[uint8(b)][2] = 1
	}

	for i, b := range conv.chars {
		//print( "chars i: ", i, " b: ", b, "\n" );
		conv.Vals[uint8(b)][0] =    uint64(i)
		conv.Vals[uint8(b)][1] = (3-uint64(i)) << (2*(uint64(kmerSize)-1))
		conv.Vals[uint8(b)][2] = 1
	}

	//print( "cleaner ", cleaner, "\n")
	//print( "res     ",     res, "\n")

	//for j, b := range vals {
	//	//fmt.Printf( "vals i: %3d b: %3d (%010b)\n", i, b, b );
	//	v, w, i := b[0], b[1], b[2]
	//	fmt.Printf( "vals i: %3d v: %3d (%010b) w: %3d (%010b) i: %d\n", j, v, v, w, w, i );
	//}
	
	return &conv
}
		









type KmerUnit struct {
	Kmer  uint64
	Count uint8
}

type KmerArr []KmerUnit

func (this *KmerArr) Print() {
	lvl, _ := logging.LogLevel("DEBUG")
	this.PrintLevel(lvl)
}

func (this *KmerArr) PrintLevel(lvl logging.Level) {
	if logging.GetLevel("seqkit") >= lvl {
		for i,j := range *this {
			//log.Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
			p.Printf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count )
		}
	}
}

func (this *KmerArr) Search(kmer uint64) int {
	return sort.Search(len(*this), func(i int) bool { return (*this)[i].Kmer >= kmer })
}

func (this *KmerArr) HasKmer(kmer uint64) bool {
	_, _, b := this.GetInfo(kmer)
	return b
}

func (this *KmerArr) GetInfo(kmer uint64) (int, KmerUnit, bool) {
	i := this.Search(kmer)
	if i == len(*this) {
		return 0, KmerUnit{0, 0}, false
	} else {
		if (*this)[i].Kmer != kmer {
			return 0, KmerUnit{0, 0}, false
		} else {
			return i, (*this)[i]    , true
		}
	}
}

func (this *KmerArr) GetIndex(kmer uint64) (int, bool) {
	i, _, b := this.GetInfo(kmer)
	return i, b
}

func (this *KmerArr) GetByKmer(kmer uint64) (KmerUnit, bool) {
	_, k, b := this.GetInfo(kmer)
	return k, b
}

func (this *KmerArr) GetByIndex(i int) KmerUnit {
	return (*this)[i]
}

func (this *KmerArr) Add(kmer uint64, LastKmerLen int) {
	//log.Debugf("KmerArr    :: Add %3d %p", kmer, (*this))

	if LastKmerLen == 0 {
		*this = append(*this, KmerUnit{kmer, 1})
	} else {
		t   := (*this)[:LastKmerLen]
		i,b := t.GetIndex(kmer)

		if b {
			addToInt8( &(*this)[i].Count, 1 )
		} else {
			*this = append(*this, KmerUnit{kmer, 1})
		}
	}

	//log.Debugf("KmerArr    :: Add %d %p", kmer, (*this))
}

func (this *KmerArr) AddSorted(length int, pos int, kmer uint64, count uint8) {
	//log.Debugf("KmerArr    :: Add %3d %p", kmer, (*this))

	if (len(*this)) != length {
		(*this) = make(KmerArr, length, length)
	}
	
	(*this)[pos] = KmerUnit{kmer, count}
	
	//log.Debugf("KmerArr    :: Add %d %p", kmer, (*this))
}

func (this *KmerArr) Clear() {
	log.Debugf("KmerArr    :: Clear %p LEN %d CAP %d", *this, len(*this), cap(*this))
	*this = (*this)[:0]
	log.Debugf("KmerArr    :: Clear %p LEN %d CAP %d", *this, len(*this), cap(*this))
}

func (this *KmerArr) isEqual(that *KmerArr) (bool, string) {
	log.Debugf("KmerArr    :: isEqual", *this, len(*this), cap(*this), *that, len(*that), cap(*that))

	if len(*this) != len(*that) {
		log.Debugf("KmerArr    :: isEqual :: Sizes differ")
		return false, "Sizes differ"
	}
	
	for i,j := range *this {
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












type KmerHolder struct {
	KmerSize      int
	KmerLen       int
	KmerCap       int
	LastKmerLen   int
	Kmer         KmerArr
}

func (this *KmerHolder) Print() {	
	log.Debugf(p.Sprintf( "kmerSize     %12d\n", this.KmerSize  ))
	log.Debugf(p.Sprintf( "KmerLen      %12d\n", this.KmerLen   ))
	log.Debugf(p.Sprintf( "KmerCap      %12d\n", this.KmerCap   ))
	log.Debugf(p.Sprintf( "Kmer         %12d CAP %12d\n", len(this.Kmer), cap(this.Kmer) ))
	this.Kmer.Print()
}


func (this *KmerHolder) Sort() {
	if len(this.Kmer) == 0 {
		println("empty")
		return
	}
	
	if len(this.Kmer) < ( 9 * (cap(this.Kmer) / 10)) {
		return
	} else {
		this.SortAct()
	}
}

func (this *KmerHolder) SortAct() {
	//https://stackoverflow.com/questions/28999735/what-is-the-shortest-way-to-simply-sort-an-array-of-structs-by-arbitrary-field
	
	if this.KmerLen == this.LastKmerLen {
		println("no growth")
		return
	} else {
		println("sort", this.KmerLen, this.LastKmerLen, len(this.Kmer), cap(this.Kmer))
	}
	
	//lvlD, _ := logging.LogLevel("DEBUG")
	lvlI, _ := logging.LogLevel("INFO" )
	
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Before :: %p Len %3d Cap %3d Prop %6.2f LastKmerLen %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastKmerLen)
	
	lasti           := 0
	//lenBufferBefore := len(this.Kmer)
	sumCountBefore  := 0

	var dstKmer  *KmerUnit
	var srcKmer  *KmerUnit
	
	var dstKmerK *uint64
	var srcKmerK *uint64
	
	var dstKmerC *uint8
	var srcKmerC *uint8

	var minK      uint64 = MaxUint
	var maxK      uint64 = 0
	
	if this.LastKmerLen == 0 { // first adding
		log.Infof("KmerArr    :: Sort :: All")

		sortSlice(&this.Kmer) // sort buffer

		for i,_ := range this.Kmer {
			dstKmer  = &this.Kmer[lasti]
			srcKmer  = &this.Kmer[i    ]

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
					//*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
					addToInt8(dstKmerC, *srcKmerC)
				} else { // different key
					lasti++ // next last
					// swap next and continue loop
					this.Kmer[lasti], this.Kmer[i] = this.Kmer[i], this.Kmer[lasti]
				}
			}
		}
	} else {
		//for i,_ := range this.Kmer { //sum buffer
		//	sumCountBefore += int(this.Kmer[i].Count)
		//}

		indexDst := 0
		indexSrc := this.LastKmerLen
	
		lenDst   := this.LastKmerLen
		lenSrc   := len(this.Kmer)

		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Before")
		//this.Kmer.Print()

		sortSliceOffset(&this.Kmer, lenDst)
		
		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: During")
		//this.Kmer.Print()
		
		for {
			if indexSrc == lenSrc { // no more buffer. stop
				//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%03d) == lenSrc (%03d). breaking", indexSrc, lenSrc )
				break
			} else { // still has buffer
				//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: indexSrc (%03d) != lenSrc (%03d). merging", indexSrc, lenSrc )
				
				dstKmer  = &this.Kmer[indexDst]
				srcKmer  = &this.Kmer[indexSrc]
				
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
					//*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
					addToInt8(dstKmerC, *srcKmerC)
					
					//next src
					indexSrc++
					
				} else if *dstKmerK < *srcKmerK { //db < buffer
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order")
					
					if indexDst >= lenDst {
						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Swapping :: indexDst %03d LastKmerLen %03d", indexDst, this.LastKmerLen)
						//this.Kmer.Print()

						this.Kmer[indexDst], this.Kmer[indexSrc] = this.Kmer[indexSrc], this.Kmer[indexDst]
						//*dstKmer, *srcKmer = *srcKmer, *dstKmer

						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d", indexSrc)
						//this.Kmer.Print()
						
						moveDownWhileSmall(&this.Kmer, indexSrc)
						
						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
						//this.Kmer.Print()
						
						lenDst = indexDst + 1
						indexSrc++
					} else {
						//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Out of order :: Next Dst")
						
						//this.Kmer.Print()
							
						//next db
						indexDst++
					}
				} else if *dstKmerK > *srcKmerK { //db > buffer. worst case scnenario
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping")

					//this.Kmer.Print()

					//swapping values
					this.Kmer[indexDst], this.Kmer[indexSrc] = this.Kmer[indexSrc], this.Kmer[indexDst]
					//*dstKmer, *srcKmer = *srcKmer, *dstKmer
					
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d", indexSrc)
					//this.Kmer.Print()
					
					moveDownWhileSmall(&this.Kmer, indexSrc)
					
					//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
					//this.Kmer.Print()
				}
			}
		}

		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: After :: indexDst: (%03d)", indexDst)
		//this.Kmer.Print()

		if indexDst < lenDst {
			lasti = lenDst - 1
		} else {
			lasti = indexDst
		}
	}

	if lasti != len(this.Kmer) - 1 {
		//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Trimming :: last I %3d Len Buffer %3d", lasti, len(this.Kmer))
		this.Kmer = this.Kmer[:lasti+1]
	}
	
	//sumCountAfter := 0
	//for i,_ := range this.Kmer {
	//	sumCountAfter += int(this.Kmer[i].Count)
	//}
	
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Len Buffer Before %3d", lenBufferBefore   )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Last Buffer Len   %3d", this.LastKmerLen)
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Last I            %3d", lasti             )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Sum B             %3d", sumCountBefore    )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Sum A             %3d", sumCountAfter     )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Len               %3d", len(this.Kmer)  )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Cap               %3d", cap(this.Kmer)  )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Min K             %3d", minK              )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Max K             %3d", maxK              )
	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: %p", this.Kmer)

	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: Final")

	//this.Kmer.PrintLevel(lvlD)

	//if sumCountBefore != sumCountAfter {
	//	this.Kmer.PrintLevel(lvlI)
	//	log.Debugf("sum differs")
	//}
	
	if len(this.Kmer) >= ( 4 * (cap(this.Kmer) / 5)) {
		newCap := (cap(this.Kmer) / 4 * 6)

		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: Len     %d", len(this.Kmer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: Cap     %d", cap(this.Kmer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: New Cap %d", newCap)
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Before :: Address %p", this.Kmer)
		
		t := make(KmerArr, len(this.Kmer), newCap)
		copy(t, this.Kmer)
		this.Kmer = t
		
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: Len     %d", len(this.Kmer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: Cap     %d", cap(this.Kmer))
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: New Cap %d", newCap)
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: After  :: Address %p", this.Kmer)
		
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: Running GC")
		runtime.GC()
		log.Infof("KmerArr    :: Merge :: Merge & Sort :: Extend :: GC Run")
	}
	
	//this.Kmer.Print()

	if len(this.Kmer) < this.LastKmerLen {
		this.Kmer.PrintLevel(lvlI)
		log.Panicf("BUFFER REDUCED SIZE")
	}
	
	this.KmerLen     = len(this.Kmer)
	this.LastKmerLen = len(this.Kmer)

	//log.Debugf("KmerArr    :: Merge :: Merge & Sort :: After  :: %p Len %3d Cap %3d Prop %6.2f LastKmerLen %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastKmerLen)
	//this.Kmer.PrintLevel(lvlI)
}

func (this *KmerHolder) Add(kmer uint64) {
	this.Sort()
	this.Kmer.Add(kmer, this.LastKmerLen)
	this.KmerLen = len(this.Kmer)
}

func (this *KmerHolder) Close() {
	this.SortAct()
	//lvlI, _ := logging.LogLevel("INFO")
	//this.Kmer.PrintLevel(lvlI)
}

func (this *KmerHolder) HasKmer(kmer uint64) bool {
	this.SortAct()
	return this.Kmer.HasKmer(kmer)
}

func (this *KmerHolder) GetInfo(kmer uint64) (int, KmerUnit, bool) {
	this.SortAct()
	return this.Kmer.GetInfo(kmer)
}

func (this *KmerHolder) GetIndex(kmer uint64) (int, bool) {
	this.SortAct()
	return this.Kmer.GetIndex(kmer)
}

func (this *KmerHolder) GetByKmer(kmer uint64) (KmerUnit, bool) {
	this.SortAct()
	return this.Kmer.GetByKmer(kmer)
}

func (this *KmerHolder) GetByIndex(i int) KmerUnit {
	this.SortAct()
	return this.Kmer.GetByIndex(i)
}

func (this *KmerHolder) AddSorted(length int, pos int, kmer uint64, count uint8) {
	this.Kmer.AddSorted(length, pos, kmer, count)
}




func NewKmerHolder(kmerSize int) *KmerHolder {
	max_kmer_size := (2 << (uint(kmerSize)*2)) / 2
	
	kmer_cap      := max_kmer_size / 100
	
	if kmer_cap < min_capacity {
		kmer_cap = min_capacity
	}
	
	var k KmerHolder     = KmerHolder{}
	    k.KmerSize       = kmerSize
	    k.KmerCap        = kmer_cap
	    k.KmerLen        = 0
	    k.LastKmerLen    = 0
	    k.Kmer           = make(KmerArr, 0, k.KmerCap  )

	log.Infof(p.Sprintf( "max db size %12d\n", max_kmer_size ))
	log.Infof(p.Sprintf( "kmer size   %12d\n", k.KmerSize    ))
	log.Infof(p.Sprintf( "kmer cap    %12d\n", k.KmerCap     ))
	
	return &k
}
