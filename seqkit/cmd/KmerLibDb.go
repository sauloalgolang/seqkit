package cmd

import (
	"sort"
	"github.com/shenwei356/go-logging"
)

type KmerUnit struct {
	Kmer  uint64
	Count uint8
}

type KmerDb []KmerUnit

func (this *KmerDb) Print() {
	lvl, _ := logging.LogLevel("DEBUG")
	this.PrintLevel(lvl)
}

func (this *KmerDb) PrintLevel(lvl logging.Level) {
	if logging.GetLevel("seqkit") >= lvl {
		for i,j := range *this {
			//log.Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
			p.Printf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count )
		}
	}
}

func (this *KmerDb) Search(kmer uint64) int {
	return sort.Search(len(*this), func(i int) bool { return (*this)[i].Kmer >= kmer })
}

func (this *KmerDb) HasKmer(kmer uint64) bool {
	_, _, b := this.GetInfo(kmer)
	return b
}

func (this *KmerDb) GetInfo(kmer uint64) (int, KmerUnit, bool) {
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

func (this *KmerDb) GetIndex(kmer uint64) (int, bool) {
	i, _, b := this.GetInfo(kmer)
	return i, b
}

func (this *KmerDb) GetByKmer(kmer uint64) (KmerUnit, bool) {
	_, k, b := this.GetInfo(kmer)
	return k, b
}

func (this *KmerDb) GetByIndex(i int) KmerUnit {
	return (*this)[i]
}

func (this *KmerDb) Add(kmer uint64, LastKmerLen int) {
	//log.Debugf("KmerDb    :: Add %3d %p", kmer, (*this))

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

	//log.Debugf("KmerDb    :: Add %d %p", kmer, (*this))
}

func (this *KmerDb) AddSorted(kmer uint64, count uint8) {
	log.Debugf("KmerDb    :: AddSorted %12d %3d %p", kmer, count, (*this))

	if (cap(*this)) < min_capacity {
		log.Debugf("KmerDb    :: AddSorted :: creating. len %12d cap %12d new cap %12d - %p", len(*this), cap(*this), min_capacity, (*this))
		(*this) = make(KmerDb, 0, min_capacity)
	} else {
		if len(*this) >= ( 9 * (cap(*this) / 10)) {
			newCap := (cap(*this) / 4 * 6)
			log.Debugf("KmerDb    :: AddSorted :: expanding. len %12d cap %12d new cap %12d - %p", len(*this), cap(*this), newCap, (*this))
			t := make(KmerDb, len(*this), newCap)
			copy(t, *this)
			(*this) = t
			log.Debugf("KmerDb    :: AddSorted :: expanding. len %12d cap %12d         %12s - %p", len(*this), cap(*this), "", (*this))
		}
	}
	
	*this = append(*this, KmerUnit{kmer, count})
	
	log.Debugf("KmerDb    :: AddSorted %12d %3d %p - added", kmer, count, (*this))
}

func (this *KmerDb) Clear() {
	log.Debugf("KmerDb    :: Clear %p LEN %d CAP %d", *this, len(*this), cap(*this))
	*this = (*this)[:0]
	log.Debugf("KmerDb    :: Clear %p LEN %d CAP %d", *this, len(*this), cap(*this))
}

func (this *KmerDb) isEqual(that *KmerDb) (bool, string) {
	log.Debugf("KmerDb    :: isEqual", *this, len(*this), cap(*this), *that, len(*that), cap(*that))

	if len(*this) != len(*that) {
		log.Debugf("KmerDb    :: isEqual :: Sizes differ")
		return false, "Sizes differ"
	}
	
	for i,j := range *this {
		if j.Kmer != (*that)[i].Kmer {
			log.Debugf("KmerDb    :: isEqual :: Kmer out of order")
			return false, "Kmer out of order"
		}
		if j.Count != (*that)[i].Count {
			log.Debugf("KmerDb    :: isEqual :: Kmer count differ")
			return false, "Kmer count differ"
		}
	}

	log.Debugf("KmerDb    :: isEqual :: OK")
	return true, "OK"
}








func sortSlice(this *KmerDb) {
	sortSliceOffset(this, 0)
}

func sortSliceOffset(this *KmerDb, offset int) {
	sort.Slice((*this)[offset:], func(i, j int) bool {
		return (*this)[offset+i].Kmer < (*this)[offset+j].Kmer
	})
}

func moveDownWhileSmall(this *KmerDb, offset int) {
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

//func mergeSortedSliceValues(this *KmerDb) int {
//	var dstKmer  *KmerUnit
//	var srcKmer  *KmerUnit
//
//	lasti   := 0
//	srcKmer  = &(*this)[lasti]
//	for i,_ := range (*this) {
//		if i != lasti {
//			dstKmer  = &(*this)[i]
//
//			if dstKmer.Kmer == (*this)[lasti].Kmer {
//				//srcKmer.Count = sumInt8( srcKmer.Count, dstKmer.Count )
//				addToInt8(&srcKmer.Count, dstKmer.Count)
//			} else {
//				lasti++
//				(*this)[lasti], (*this)[i] = (*this)[i], (*this)[lasti]
//				srcKmer = &(*this)[lasti]
//				//*srcKmer, *dstKmer = *dstKmer, *srcKmer
//			}
//		}
//	}
//	return lasti
//}




//func sumInt8( a, b uint8 ) uint8 {
//	t := a
//	addToInt8( &t, b )
//	return t
//}

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

