package cmd

import (
	"sort"
	"runtime"
)

type KmerUnit struct {
	Kmer  uint64
	Count uint8
}

type KmerDb []KmerUnit

func NewKmerDB() (k KmerDb) {
	k = make(KmerDb, 0, 0)
	return k
}

func (this *KmerDb) Print() {
	this.PrintLevel("DEBUG")
}

func (this *KmerDb) PrintLevel(lvl string) {
	if IsLogLevelValid(lvl) {
		for i,j := range *this {
			//Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
			PrintLevelf( lvl, "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count )
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

func (this *KmerDb) GetInfo(kmer uint64) (int, *KmerUnit, bool) {
	i := this.Search(kmer)
	if i >= len(*this) {
		return 0, &KmerUnit{0, 0}, false
	} else {
		if (*this)[i].Kmer == kmer {
			return i, &(*this)[i]    , true
		} else {
			return 0, &KmerUnit{0, 0}, false
		}
	}
}

func (this *KmerDb) GetIndex(kmer uint64) (int, bool) {
	i, _, b := this.GetInfo(kmer)
	return i, b
}

func (this *KmerDb) GetByKmer(kmer uint64) (*KmerUnit, bool) {
	_, k, b := this.GetInfo(kmer)
	return k, b
}

func (this *KmerDb) GetByIndex(i int) *KmerUnit {
	return &(*this)[i]
}

func (this *KmerDb) Merge(that *KmerDb, LastKmerLen int) {
	Debug("KmerDb :: Merging")
	
	if len(*this) == 0 {
		Debug("KmerDb :: Merging :: first - just copying ", len(*this), len(*that))
		this.Replace(that)
		Debug("KmerDb :: Merging :: first - copied ", len(*this), len(*that))
	} else {
		Debug("KmerDb :: Merging :: joining ", len(*this), len(*that))
		var pos    int  = 0
		var lkp    int  = 0
		var index  int  = 0
		var found  bool = false
		var src   *KmerUnit

		for pos = 0; pos < len(*that); pos++ {
			src = &(*that)[pos]
			
			index, found = this.AddWithKnowledge(src.Kmer, src.Count, LastKmerLen, lkp)
			
			//println("pos ", pos, " kmer ", src.Kmer, " count ", src.Count, " LastKmerLen ", LastKmerLen, " lkp ", lkp, " index ", index, " found ", found)
			
			if found {
				//println("found ", (*this)[index].Kmer)
				lkp = index + 1
			}
		}
		Debug("KmerDb :: Merging :: joined ", len(*this), len(*that))
	}
	Debug("KmerDb :: Merged")
}

func (this *KmerDb) Add(kmer uint64, count uint8, LastKmerLen int) {
	//Debugf("KmerDb    :: Add %3d %p", kmer, (*this))
	this.AddWithKnowledge(kmer, count, LastKmerLen, 0)
}

func (this *KmerDb) AddWithKnowledge(kmer uint64, count uint8, LastKmerLen int, lastKnownPlace int) (int, bool) {
	if LastKmerLen == 0 {
		//print("N")
		this.Append(kmer, count)
		return 0, false
	} else {
		t           := (*this)[lastKnownPlace:LastKmerLen]
		index,found := t.GetIndex(kmer)

		if found {
			//print("S")
			addToInt8( &(*this)[lastKnownPlace+index].Count, count )
		} else {
			//print("A")
			this.Append(kmer, count)
		}
		
		return lastKnownPlace+index,found
	}
	//Debugf("KmerDb    :: Add %d %p", kmer, (*this))
}

func (this *KmerDb) Append(kmer uint64, count uint8) {
	if this.Is90Percent() {
		this.Extend()
	}
	*this = append(*this, KmerUnit{kmer, count})
}

func (this *KmerDb) Replace(that *KmerDb) {
	Debug("replacing")
	
	t := make(KmerDb, len(*that), cap(*that)+max_capacity)
	copy(t, *that)
	(*this) = t
	
	Debug("replaced. running gc")
	runtime.GC()
	Debug("replaced. running gc finished")
}

func (this *KmerDb) Extend() {
	Debug("extending ", len(*this), cap(*this))
	
	newSize := len(*this)
	newCap  := ((cap(*this) / 4) * 6) //50%

	if newCap - cap(*this) < min_capacity {
		newCap = cap(*this) + min_capacity		
	}
	
	if newCap - cap(*this) > max_capacity {
		newCap = cap(*this) + max_capacity
	}
		
	t := make(KmerDb, newSize, newCap)
	copy(t, *this)
	(*this) = t

	Debug("extended  ", len(*this), cap(*this))
	Debug("extended. running  gc")

	runtime.GC()

	Debug("extended. running  gc finished")
}

func (this *KmerDb) Is80Percent() (iaf bool) {
	iaf = len(*this) >= ( (cap(*this) / 10) * 8 )
	return
}

func (this *KmerDb) Is90Percent() (iaf bool) {
	iaf = len(*this) >= ( (cap(*this) / 10) * 9 )
	return
}

func (this *KmerDb) AddSorted(kmer uint64, count uint8) {
	Debugf("KmerDb    :: AddSorted %12d %3d %p", kmer, count, (*this))

	if (cap(*this)) < min_capacity {
		Debugf("KmerDb    :: AddSorted :: creating. len %12d cap %12d new cap %12d - %p", len(*this), cap(*this), min_capacity, (*this))
		(*this) = make(KmerDb, 0, min_capacity)
	} else {
		if len(*this) >= ( 9 * (cap(*this) / 10)) {
			newCap := (cap(*this) / 4 * 6)
			
			if newCap - cap(*this) > max_capacity {
				newCap = cap(*this) + max_capacity
			}
			
			Debugf("KmerDb    :: AddSorted :: expanding. len %12d cap %12d new cap %12d - %p", len(*this), cap(*this), newCap, (*this))
			t := make(KmerDb, len(*this), newCap)
			copy(t, *this)
			(*this) = t
			Debugf("KmerDb    :: AddSorted :: expanding. len %12d cap %12d         %12s - %p", len(*this), cap(*this), "", (*this))
		}
	}
	
	*this = append(*this, KmerUnit{kmer, count})
	
	Debugf("KmerDb    :: AddSorted %12d %3d %p - added", kmer, count, (*this))
}

func (this *KmerDb) Clear() {
	Debugf("KmerDb    :: Clear %p LEN %d CAP %d", *this, len(*this), cap(*this))
	*this = (*this)[:0]
	Debugf("KmerDb    :: Clear %p LEN %d CAP %d", *this, len(*this), cap(*this))
}

func (this *KmerDb) isEqual(that *KmerDb) (bool, string) {
	Debugf("KmerDb    :: isEqual", *this, len(*this), cap(*this), *that, len(*that), cap(*that))

	if len(*this) != len(*that) {
		Debugf("KmerDb    :: isEqual :: Sizes differ")
		return false, "Sizes differ"
	}
	
	for i,j := range *this {
		if j.Kmer != (*that)[i].Kmer {
			Debugf("KmerDb    :: isEqual :: Kmer out of order")
			return false, "Kmer out of order"
		}
		if j.Count != (*that)[i].Count {
			Debugf("KmerDb    :: isEqual :: Kmer count differ")
			return false, "Kmer count differ"
		}
	}

	Debugf("KmerDb    :: isEqual :: OK")
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
	Debug("moveDownWhileSmall ", offset)
	
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

