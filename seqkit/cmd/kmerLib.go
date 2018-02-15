package cmd

import (
	"runtime"
	"sort"
	"encoding/binary"
	"io"
	"fmt"
	"golang.org/x/text/message"
	"github.com/shenwei356/go-logging"
	"github.com/shenwei356/xopen"
)

const min_capacity = 1000000
const max_counter = uint8(254)
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
	if *a < max_counter {
		//print("<", *a, " ", max_counter)
		if (max_counter - *a) <= b {
			//println("-")
			*a = max_counter
		} else {
			//println("+")
			*a += b
		}
	} else {
		//println("=", *a, " ", max_counter)
		*a = max_counter
	}
}









type KmerIO struct {
	OutFh *xopen.Writer
	InFh *xopen.Reader
	mode int
	buf []byte
}

func (this *KmerIO) initWriter(outFh *xopen.Writer) {
	if this.mode != 0 {
		log.Panic("writing on open file")
	}
	this.buf        = make([]byte, binary.MaxVarintLen64)
	this.OutFh      = outFh
	this.mode       = 1
}

func (this *KmerIO) initReader(inFh *xopen.Reader) {
	if this.mode != 0 {
		log.Panic("reading on open file")
	}
	this.buf        = make([]byte, binary.MaxVarintLen64)
	this.InFh       = inFh
	this.mode       = 2
}

func (this *KmerIO) CheckMode(mode int) {
	if mode == 1 {
		if this.mode == 0 {
			log.Panic("writing on closed file")
		}
		if this.mode == 2 {
			log.Panic("writing on reading file")
		}
	} else if mode == 2 {
		if this.mode == 0 {
			log.Panic("reading on closed file")
		}
		if this.mode == 1 {
			log.Panic("reading on reading file")
		}		
	}
}

func (this *KmerIO) ReadUint8(res *uint8) (bool) {
	this.CheckMode(2)
	
	err := binary.Read(this.InFh, binary.LittleEndian, res);

	if err != nil {
		if err == io.EOF {
			return false
		} else {
			log.Panic("binary.Read failed:", err)
		}
	}
	
	return true
}

func (this *KmerIO) ReadUint64(res *uint64) (bool) {
	this.CheckMode(2)
	
	err := binary.Read(this.InFh, binary.LittleEndian, res);

	if err != nil {
		if err == io.EOF {
			return false
		} else {
			log.Panic("binary.Read failed:", err)
		}
	}
	
	return true
}

func (this *KmerIO) ReadUint64V() (uint64, bool) {
	this.CheckMode(2)

	i, err := binary.ReadUvarint(this.InFh);

	if err != nil {
		if err == io.EOF {
			return 0, false
		} else {
			log.Panic("binary.Read failed:", err)
		}
	}
	
	return i, true
}

func (this *KmerIO) WriteUint8(x uint8) {
	this.CheckMode(1)

	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])

	err := binary.Write(this.OutFh, binary.LittleEndian, x)

	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteUint64(x uint64) {
	this.CheckMode(1)

	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])

	err := binary.Write(this.OutFh, binary.LittleEndian, x)

	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteUint64V(x uint64) {
	this.CheckMode(1)
	
	n := binary.PutUvarint(this.buf, x)
	
	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])
	
	err := binary.Write(this.OutFh, binary.LittleEndian, this.buf[:n])
	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) Flush() {
	this.CheckMode(1)

	this.OutFh.Flush()
}




type Converter struct {
	KmerSize  int
	Cleaner  uint64
    cHARS   [  4]byte
    chars   [  4]byte
	Vals    [256][3]uint64
	table   []uint64
}

func (conv Converter) NumToSeq(kmer uint64) string {
	seq := make([]byte, conv.KmerSize, conv.KmerSize)
	p   := uint64(0)
	q   := uint64(0)
	c   := byte(0)
	
	for i:=conv.KmerSize-1; i>=0; i-- {
		p = kmer & conv.table[i]
		q = p >> (uint(i)*2)
		c = conv.cHARS[q]
		
		//fmt.Printf( "i %3d - kmer %010b - a %010b - p %010b - q %010b - c %s\n"  , i, kmer, conv.table[i], p, q, string(c) )
		//fmt.Printf( "        kmer % 10d - a % 10d - p % 10d - q % 10d - c %s\n\n",    kmer, conv.table[i], p, q, string(c) )
		
		seq[uint(conv.KmerSize)-uint(i)-1] = c
	}
	
	//println("SEQ", string(seq))
	
	return string(seq)
}


func NewConverter(kmerSize int) *Converter {
	conv          := Converter{}
	conv.KmerSize  = kmerSize
	conv.Cleaner   = (1 << (uint64(kmerSize)*2)) - 1
	conv.Vals      = [256][3]uint64{}
    conv.cHARS     = [ 4]byte{'A', 'C', 'G', 'T'}
    conv.chars     = [ 4]byte{'a', 'c', 'g', 't'}
	conv.table     = make([]uint64, kmerSize)
	
	for i, _ := range conv.Vals {
		for j, _ := range conv.Vals[i] {
			conv.Vals[i][j] = 0
		}
	}

	for i, b := range conv.cHARS {
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

	for i:=conv.KmerSize-1; i>=0; i-- {
		conv.table[i] = (^uint64(0)) & (3 << (uint(i)*2))
	}
	
	//for i:=uint64(0); i < 1024; i++ {
	//	seq := conv.NumToSeq(i)
	//	println("I", i, "seq", seq)
	//}
	
	//log.Panic("done")
	
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
		//println("empty")
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
		//println("no growth")
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

func (this *KmerHolder) ToFile(outFile string, minCount uint8) bool {
	outFh, err := xopen.Wopen(outFile)
	checkError(err)
	defer outFh.Close()
	return this.ToFileHandle(outFh, minCount)
}

func (this *KmerHolder) ToFileHandle(outFh *xopen.Writer, minCount uint8) bool {
	println("saving to stream")
	
	kio := KmerIO{}
	kio.initWriter(outFh)
	
	var kmer     uint64 = 0
	var count    uint8  = 0
	var lastKmer uint64 = 0
	var kmerdiff uint64 = 0
	
	var numK     uint64 = 0
	var numV     uint64 = 0
	var minK     uint64 = MaxUint
	var maxK     uint64 = 0
	var minC     uint8  = 254
	var maxC     uint8  = 0
	var minD     uint64 = MaxUint
	var maxD     uint64 = 0
	var sumC     uint64 = 0
	var sumD     uint64 = 0

	var regs     uint64 = uint64(len(this.Kmer))
	fmt.Printf("writing %12d registers\n", regs)
	fmt.Printf("writing %12d minimun count\n", minCount)

	kio.WriteUint64(regs)
	kio.WriteUint8(minCount)
	
	for k, _ := range this.Kmer {
		kmer, count = this.Kmer[k].Kmer, this.Kmer[k].Count

		numK++

		if count < minCount {
			//kio.WriteUint64V(0)
			kio.WriteUint64(0)
			kio.WriteUint8(0)
			continue
		}

		numV++
		kmerdiff    = kmer - lastKmer
		if kmer     < minK { minK = kmer     }
		if kmer     > maxK { maxK = kmer     }
		if count    < minC { minC = count    }
		if count    > maxC { maxC = count    }
		if kmerdiff < minD { minD = kmerdiff }
		if kmerdiff > maxD { maxD = kmerdiff }
		
		sumC += uint64(count)
		sumD += kmerdiff
		
		//fmt.Printf("W k %d kmer %d count %d kmerdiff %d\n", k, kmer, count, kmerdiff)
		kio.WriteUint64(kmerdiff)
		//kio.WriteUint64V(kmerdiff)
		kio.WriteUint8(count)
		lastKmer = kmer
	}

	fmt.Printf("WRITE registers: %12d valid: %12d :: kmer min: %12d max: %12d :: diff sum: %12d min: %12d max: %12d :: count sum: %12d min: %12d max: %12d\n",
			   numK, numV, minK, maxK, sumD, minD, maxD, sumC, minC, maxC )

	kio.WriteUint64(numV)
	kio.WriteUint64(minK)
	kio.WriteUint64(maxK)
	kio.WriteUint8 (minC)
	kio.WriteUint8 (maxC)
	kio.WriteUint64(minD)
	kio.WriteUint64(maxD)
	kio.WriteUint64(sumC)
	kio.WriteUint64(sumD)

	if numK != regs {
		log.Panicf("number of writen registers not the same as expected. %d vs %d", numK, regs)
	}

	kio.Flush()
	
	return false
}

func (this *KmerHolder) FromFile(inFile string) bool {
	inFh, err := xopen.Ropen(inFile)
	checkError(err)
	defer inFh.Close()
	return this.FromFileHandle(inFh)
}

func (this *KmerHolder) FromFileHandle(inFh *xopen.Reader) bool {
	println("reading from stream")

	kio := KmerIO{}
	kio.initReader(inFh)

	var kmer     uint64 = 0
	var count    uint8  = 0
	var lastKmer uint64 = 0
	var kmerdiff uint64 = 0
	var succes   bool

	var regs     uint64 = 0
	var minCount uint8  = 0

	var numK     uint64 = 0
	var numV     uint64 = 0
	var minK     uint64 = MaxUint
	var maxK     uint64 = 0
	var minC     uint8  = 254
	var maxC     uint8  = 0
	var minD     uint64 = MaxUint
	var maxD     uint64 = 0
	var sumC     uint64 = 0
	var sumD     uint64 = 0

	succes = kio.ReadUint64(&regs)
	if !succes { log.Panic("error reading begining of the file") }
	succes = kio.ReadUint8(&minCount)
	if !succes { log.Panic("error reading begining of the file") }
	
	fmt.Printf("reading %12d registers\n", regs)
	fmt.Printf("reading %12d minimum count\n", minCount)
	
	for {
		succes = kio.ReadUint64(&kmerdiff)
		//kmerdiff, succes = kio.ReadUint64V()
		if !succes { break }
		
		succes = kio.ReadUint8(&count)
		if !succes { break }
		
		numK++
		
		if kmerdiff == 0 && count == 0 {
			if numK == regs {
				break
			} else {
				continue
			}
		}

		numV++

		kmer          = lastKmer + kmerdiff

		if kmer     < minK { minK = kmer     }
		if kmer     > maxK { maxK = kmer     }
		if count    < minC { minC = count    }
		if count    > maxC { maxC = count    }
		if kmerdiff < minD { minD = kmerdiff }
		if kmerdiff > maxD { maxD = kmerdiff }
		
		sumC += uint64(count)
		sumD += kmerdiff
		
		//fmt.Printf("R k %d kmer %d count %d kmerdiff %d\n", numK, kmer, count, kmerdiff)
		
		lastKmer = kmer
		
		if numK == regs {
			break
		}
	}

	fmt.Printf("READ  registers: %12d valid: %12d :: kmer min: %12d max: %12d :: diff sum: %12d min: %12d max: %12d :: count sum: %12d min: %12d max: %12d\n",
		   numK, numV, minK, maxK, sumD, minD, maxD, sumC, minC, maxC )
	
	if numK != regs { log.Panicf("number of read registers not the same as expected. %d vs %d", numK, regs) }
	
	var numVc    uint64 = 0
	var minKc    uint64 = MaxUint
	var maxKc    uint64 = 0
	var minCc    uint8  = 254
	var maxCc    uint8  = 0
	var minDc    uint64 = MaxUint
	var maxDc    uint64 = 0
	var sumCc    uint64 = 0
	var sumDc    uint64 = 0

	
	succes = kio.ReadUint64(&numVc)
	if !succes { log.Panic("error reading end of the file") }
	if numV != numVc { log.Panicf("number of valid kmer not the same as expected. %d vs %d", numV, numVc) }
	
	succes = kio.ReadUint64(&minKc)
	if !succes { log.Panic("error reading end of the file") }
	if minK != minKc { log.Panicf("minimal kmer not the same as expected. %d vs %d", minK, minKc) }
	
	succes = kio.ReadUint64(&maxKc)
	if !succes { log.Panic("error reading end of the file") }
	if maxK != maxKc { log.Panicf("maximum kmer not the same as expected. %d vs %d", maxK, maxKc) }
	
	succes = kio.ReadUint8(&minCc)
	if !succes { log.Panic("error reading end of the file") }
	if minC != minCc { log.Panicf("minimal kmer not the same as expected. %d vs %d", minC, minCc) }
	
	succes = kio.ReadUint8(&maxCc)
	if !succes { log.Panic("error reading end of the file") }
	if maxC != maxCc { log.Panicf("maximum kmer not the same as expected. %d vs %d", maxC, maxCc) }

	succes = kio.ReadUint64(&minDc)
	if !succes { log.Panic("error reading end of the file") }
	if minD != minDc { log.Panicf("minimal kmer diff not the same as expected. %d vs %d", minD, minDc) }
	
	succes = kio.ReadUint64(&maxDc)
	if !succes { log.Panic("error reading end of the file") }
	if maxD != maxDc { log.Panicf("maximum kmer diff not the same as expected. %d vs %d", maxD, maxDc) }

	succes = kio.ReadUint64(&sumCc)
	if !succes { log.Panic("error reading end of the file") }
	if sumC != sumCc { log.Panicf("sum of counts not the same as expected. %d vs %d", sumC, sumCc) }

	succes = kio.ReadUint64(&sumDc)
	if !succes { log.Panic("error reading end of the file") }
	if sumD != sumDc { log.Panicf("sum of diff not the same as expected. %d vs %d", sumD, sumDc) }
		
	return false
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
