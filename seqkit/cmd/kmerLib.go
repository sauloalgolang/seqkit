package cmd

import (
	"runtime"
	"sync"
	"time"
)

const min_capacity = 1000000
const max_counter  = uint8(254)
//https://stackoverflow.com/questions/6878590/the-maximum-value-for-an-int-type-in-go
const MaxUint      = ^uint64(0) 
const MinUint      = 0
const MaxInt       = int64(MaxUint >> 1)
const MinInt       = -MaxInt - 1



type KmerHolder struct {
	KmerSize      int
	MinLen        int
	MaxLen        int
	KmerCap       int
	KmerCount     int
	NumKmers      int
	LastNumKmers  int
	Profile       bool
	LastPrint     time.Time
	StartTIme     time.Time
	Kmer          KmerDb
	ParserG      *KmerParser
	ReadStatsG   *KmerReadStat
	mux           sync.Mutex
	wg            sync.WaitGroup
}

func NewKmerHolder(kmerSize, minLen, maxLen int, profile bool) (k *KmerHolder) {
	max_kmer_size := (2 << (uint(kmerSize)*2)) / 2
	
	kmer_cap      := max_kmer_size / 100
	
	if kmer_cap < min_capacity {
		kmer_cap = min_capacity
	}
	
	k              = new(KmerHolder)
	k.KmerSize     = kmerSize
	k.MinLen       = minLen
	k.MaxLen       = maxLen
	k.KmerCap      = kmer_cap
	k.KmerCount    = 0
	k.NumKmers     = 0
	k.LastNumKmers = 0
	k.Profile      = profile
	k.LastPrint    = time.Now()
	k.StartTIme    = k.LastPrint
	k.Kmer         = make(KmerDb, 0, kmer_cap)
	k.ParserG      = NewKmerParser(k.KmerSize, k.MinLen, k.MaxLen, k.Profile, k.Add)
	k.ReadStatsG   = NewKmerReadStat()
	k.mux          = sync.Mutex{}
	k.wg           = sync.WaitGroup{}

	if k.Profile {
		Info( "profiling" )
	}

	Infof( "max db size %12d\n", max_kmer_size )
	Infof( "kmer size   %12d\n", k.KmerSize    )
	Infof( "kmer cap    %12d\n", k.KmerCap     )
	
	return k
}

func (this *KmerHolder) Print() {	
	Debugf( "kmerSize     %12d\n", this.KmerSize  )
	Debugf( "NumKmers     %12d\n", this.NumKmers  )
	Debugf( "KmerCap      %12d\n", this.KmerCap   )
	Debugf( "Kmer         %12d CAP %12d\n", len(this.Kmer), cap(this.Kmer) )

	this.Kmer.Print()
}

func (this *KmerHolder) Add(kmer uint64, count uint8) {
	this.mux.Lock()
	defer this.mux.Unlock()

	this.KmerCount++
	
	if this.KmerCount % 1000000 == 0 {
		tnow  := time.Now()
		tdiff := tnow.Sub(this.LastPrint)
		tinit := tnow.Sub(this.StartTIme)
		
		Infof( "%12d %v %v\n", this.KmerCount, tinit, tdiff )
		
		this.LastPrint      = time.Now()
	}
	
	this.Sort()
	this.Kmer.Add(kmer, count, this.LastNumKmers)
	this.NumKmers = len(this.Kmer)
}

func (this *KmerHolder) Merge(that *KmerHolder) {
	
}

func (this *KmerHolder) ParseFastQ(key1 string, key2 string, seq *[]byte) {
	// not necessary but why not?
	this.wg.Add(1)
	defer this.wg.Done()

	this.mux.Lock()
	defer this.mux.Unlock()

	s := this.ParserG.fast(seq, FASTQ)
	this.ReadStatsG.AddSS(key1, key2, *s)
}

func (this *KmerHolder) ParseFastA(key1 string, key2 []byte, seq *[]byte) {
	this.wg.Add(1)

	go func() {
		defer this.wg.Done()

		k := NewKmerHolder(this.KmerSize, this.MinLen, this.MaxLen, this.Profile)
		//p := NewKmerParser(this.KmerSize, this.MinLen, this.MaxLen, this.Profile, this.Add)
		p := NewKmerParser(this.KmerSize, this.MinLen, this.MaxLen, this.Profile, k.Add)
		s := p.fast(seq, FASTA)

		this.mux.Lock()
		defer this.mux.Unlock()
		this.Merge(k)
		this.ReadStatsG.AddSB(key1, key2, *s)
	}()
}

func (this *KmerHolder) Wait() {
	Info("Waiting for conclusion")
	this.wg.Wait()
	Info("Finished reading")
}

func (this *KmerHolder) Close() {
	this.SortAct()
	//this.Kmer.Print()
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

func (this *KmerHolder) AddSorted(kmer uint64, count uint8) {
	this.Kmer.AddSorted(kmer, count)
	this.NumKmers = len(this.Kmer)
}

func (this *KmerHolder) Clear() {
	this.Kmer.Clear()
	this.NumKmers     = len(this.Kmer)
	this.LastNumKmers = len(this.Kmer)
}

func (this *KmerHolder) PrintStats() {
	this.ReadStatsG.Print()
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
	
	if this.NumKmers == this.LastNumKmers {
		//println("no growth")
		return
	} else {
		Infof("KmerDb    :: Sort :: num kmers: %12d last kmer: %12d len kmer: %12d cap kmer: %12d", this.NumKmers, this.LastNumKmers, len(this.Kmer), cap(this.Kmer))
	}
	
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Before :: %p Len %3d Cap %3d Prop %6.2f LastNumKmers %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastNumKmers)
	
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
	
	if this.LastNumKmers == 0 {
		// first adding
		Info("KmerDb    :: Sort :: Add")

		// sort buffer
		sortSlice(&this.Kmer)

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
		indexSrc := this.LastNumKmers
	
		lenDst   := this.LastNumKmers
		lenSrc   := len(this.Kmer)

		//Debugf("KmerDb    :: Merge :: Merge & Sort :: Before")
		//this.Kmer.Print()

		sortSliceOffset(&this.Kmer, lenDst)
		
		//Debugf("KmerDb    :: Merge :: Merge & Sort :: During")
		//this.Kmer.Print()
		
		for {
			if indexSrc == lenSrc { // no more buffer. stop
				//Debugf("KmerDb    :: Merge :: Merge & Sort :: indexSrc (%03d) == lenSrc (%03d). breaking", indexSrc, lenSrc )
				break
			} else { // still has buffer
				//Debugf("KmerDb    :: Merge :: Merge & Sort :: indexSrc (%03d) != lenSrc (%03d). merging", indexSrc, lenSrc )
				
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
				
				//Debugf("KmerDb    :: Merge :: Merge & Sort :: dstIndex % 3d srcIndex % 3d",  indexDst,  indexSrc)
				//Debugf("KmerDb    :: Merge :: Merge & Sort :: dstKmerK %03d srcKmerK %03d", *dstKmerK, *srcKmerK)

				if *dstKmerK == *srcKmerK { //same kmer
					//Debugf("KmerDb    :: Merge :: Merge & Sort :: Adding")

					//sum
					//*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
					addToInt8(dstKmerC, *srcKmerC)
					
					//next src
					indexSrc++
					
				} else if *dstKmerK < *srcKmerK { //db < buffer
					//Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order")
					
					if indexDst >= lenDst {
						//Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Swapping :: indexDst %03d LastNumKmers %03d", indexDst, this.LastNumKmers)
						//this.Kmer.Print()

						this.Kmer[indexDst], this.Kmer[indexSrc] = this.Kmer[indexSrc], this.Kmer[indexDst]
						//*dstKmer, *srcKmer = *srcKmer, *dstKmer

						//Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d", indexSrc)
						//this.Kmer.Print()
						
						moveDownWhileSmall(&this.Kmer, indexSrc)
						
						//Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
						//this.Kmer.Print()
						
						lenDst = indexDst + 1
						indexSrc++
					} else {
						//Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Next Dst")
						
						//this.Kmer.Print()
							
						//next db
						indexDst++
					}
				} else if *dstKmerK > *srcKmerK { //db > buffer. worst case scnenario
					//Debugf("KmerDb    :: Merge :: Merge & Sort :: Swapping")

					//this.Kmer.Print()

					//swapping values
					this.Kmer[indexDst], this.Kmer[indexSrc] = this.Kmer[indexSrc], this.Kmer[indexDst]
					//*dstKmer, *srcKmer = *srcKmer, *dstKmer
					
					//Debugf("KmerDb    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d", indexSrc)
					//this.Kmer.Print()
					
					moveDownWhileSmall(&this.Kmer, indexSrc)
					
					//Debugf("KmerDb    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
					//this.Kmer.Print()
				}
			}
		}

		//Debugf("KmerDb    :: Merge :: Merge & Sort :: After :: indexDst: (%03d)", indexDst)
		//this.Kmer.Print()

		if indexDst < lenDst {
			lasti = lenDst - 1
		} else {
			lasti = indexDst
		}
	}

	if lasti != len(this.Kmer) - 1 {
		//Debugf("KmerDb    :: Merge :: Merge & Sort :: Trimming :: last I %3d Len Buffer %3d", lasti, len(this.Kmer))
		this.Kmer = this.Kmer[:lasti+1]
	}
	
	//sumCountAfter := 0
	//for i,_ := range this.Kmer {
	//	sumCountAfter += int(this.Kmer[i].Count)
	//}
	
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Len Buffer Before %3d", lenBufferBefore   )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Last Buffer Len   %3d", this.LastNumKmers)
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Last I            %3d", lasti             )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Sum B             %3d", sumCountBefore    )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Sum A             %3d", sumCountAfter     )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Len               %3d", len(this.Kmer)  )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Cap               %3d", cap(this.Kmer)  )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Min K             %3d", minK              )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Max K             %3d", maxK              )
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: %p", this.Kmer)

	//Debugf("KmerDb    :: Merge :: Merge & Sort :: Final")

	//this.Kmer.Print()

	//if sumCountBefore != sumCountAfter {
	//	this.Kmer.Print()
	//	Debugf("sum differs")
	//}
	
	if len(this.Kmer) >= ( 4 * (cap(this.Kmer) / 5)) {
		newCap := (cap(this.Kmer) / 4 * 6)

		Infof("KmerDb    :: Sort :: Extend :: Before :: Len     %d", len(this.Kmer))
		Infof("KmerDb    :: Sort :: Extend :: Before :: Cap     %d", cap(this.Kmer))
		Infof("KmerDb    :: Sort :: Extend :: Before :: New Cap %d", newCap)
		Infof("KmerDb    :: Sort :: Extend :: Before :: Address %p", this.Kmer)
		
		t := make(KmerDb, len(this.Kmer), newCap)
		copy(t, this.Kmer)
		this.Kmer = t
		
		Infof("KmerDb    :: Sort :: Extend :: After  :: Len     %d", len(this.Kmer))
		Infof("KmerDb    :: Sort :: Extend :: After  :: Cap     %d", cap(this.Kmer))
		Infof("KmerDb    :: Sort :: Extend :: After  :: New Cap %d", newCap)
		Infof("KmerDb    :: Sort :: Extend :: After  :: Address %p", this.Kmer)
		
		Infof("KmerDb    :: Sort :: Extend :: Running GC")
		runtime.GC()
		Infof("KmerDb    :: Sort :: Extend :: GC Run")
	}
	
	//this.Kmer.Print()

	if len(this.Kmer) < this.LastNumKmers {
		this.Kmer.Print()
		Panic("BUFFER REDUCED SIZE")
	}
	
	this.NumKmers     = len(this.Kmer)
	this.LastNumKmers = len(this.Kmer)
	Info("KmerDb    :: Sort :: Sorted")
	Infof("KmerDb    :: Sort :: num kmers: %12d last kmer: %12d len kmer: %12d cap kmer: %12d", this.NumKmers, this.LastNumKmers, len(this.Kmer), cap(this.Kmer))
	//Debugf("KmerDb    :: Merge :: Merge & Sort :: After  :: %p Len %3d Cap %3d Prop %6.2f LastNumKmers %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastNumKmers)
	//this.Kmer.Print()
}









func (this *KmerHolder) ToFile(outFile string, minCount uint8) bool {
	kio := NewKmerIO()
	kio.openWriter(outFile)
	//defer kio.Flush()
	defer kio.Close()
	return this.ToFileHandle(kio, minCount)
}

func (this *KmerHolder) ToFileHandle(kio *KmerIO, minCount uint8) bool {
	Info("saving to stream")

	Info("saving to stream :: sorting database")
	this.SortAct()
	Info("saving to stream :: database sorted")

	
	var kmer     uint64 = 0
	var count    uint8  = 0
	var lastKmer uint64 = 0
	var kmerdiff uint64 = 0
	
	var numK     uint64 = 0
	csk := NewChecksumK()

	var regs     uint64 = uint64(len(this.Kmer))
	Infof("saving to stream :: writing %23d registers\n"    , regs    )
	Infof("saving to stream :: writing %23d minimun count\n", minCount)

	kio.WriteUint64(regs)
	kio.WriteUint8(minCount)
	
	for k, _ := range this.Kmer {
		kmer, count = this.Kmer[k].Kmer, this.Kmer[k].Count

		numK++

		if count < minCount {
			//kio.WriteUint64(0)
			kio.WriteUint64V(0)
			kio.WriteUint8(0)
			continue
		}

		kmerdiff    = kmer - lastKmer
		
		if kmer != 0 && lastKmer != 0 {
			if kmer == lastKmer {
				Panicf("duplicated kmer. %d vs %d", kmer, lastKmer)
			}
			if kmerdiff == 0 {
				Panicf("zero difference kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
			}
		}

		if count == 0 {
			Panicf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
		}
		
		csk.Add(kmer, count, kmerdiff)
				
		//Infof("W k %d kmer %d count %d kmerdiff %d\n", k, kmer, count, kmerdiff)
		//kio.WriteUint64(kmerdiff)
		kio.WriteUint64V(kmerdiff)
		kio.WriteUint8(count)
		lastKmer = kmer
	}

	Infof("saving to stream :: registers written: %12d ", numK )
	
	csk.Print()

	if numK != regs {
		Panicf("number of writen registers not the same as expected. %d vs %d", numK, regs)
	}

	csk.Check()

	Info(p.Sprint("saving to stream :: data is valid. writing statistics"))

	kio.WriteStruct(csk)
	
	Info(p.Sprint("saving to stream :: finished"))

	return true
}

func (this *KmerHolder) FromFile(inFile string) bool {
	kio := KmerIO{}
	kio.openReader(inFile)
	defer kio.Close()
	return this.FromFileHandle(&kio)
}

func (this *KmerHolder) FromFileHandle(kio *KmerIO) bool {
	Info("reading from stream")

	Info("reading from stream :: cleaning database")
	this.Clear()
	Info("reading from stream :: database clean")
	
	csk := NewChecksumK()

	var kmer     uint64 = 0
	var count    uint8  = 0
	var lastKmer uint64 = 0
	var kmerdiff uint64 = 0
	var succes   bool

	var regs     uint64 = 0
	var minCount uint8  = 0

	var numK     uint64 = 0

	
	
	succes = kio.ReadUint64(&regs)
	if !succes { Panic("error reading begining of the file") }
	
	succes = kio.ReadUint8(&minCount)
	if !succes { Panic("error reading begining of the file") }
	
	
	
	Infof("reading from stream :: reading %20d registers\n", regs)
	Infof("reading from stream :: reading %20d minimum count\n", minCount)
	
	for {
		//succes = kio.ReadUint64(&kmerdiff)
		kmerdiff, succes = kio.ReadUint64V()
		if !succes { break }
		
		succes = kio.ReadUint8(&count)
		if !succes { break }
		
		numK++
		
		if kmerdiff == 0 {
			if count == 0 {
				if numK == regs {
					break
				} else {
					continue
				}
			} else {
				if lastKmer != 0 {
					Panicf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
				}
			}
		}
		
		if count == 0 {
			Panicf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
		}

		kmer          = lastKmer + kmerdiff

		csk.Add(kmer, count, kmerdiff)

		this.AddSorted(kmer, count)
		
		//Infof("R k %d kmer %d count %d kmerdiff %d\n", numK, kmer, count, kmerdiff)
		
		lastKmer = kmer
		
		if numK == regs {
			break
		}
	}

	Infof("reading from stream :: registers read: %12d ", numK)
	
	Infof("reading from stream :: statistic of data acquired:")
	csk.Print()

	if numK != regs { Panicf("number of read registers not the same as expected. %d vs %d", numK, regs) }

	csk.Check()


	Infof("reading from stream :: reading statistics in file")

	cskC := NewChecksumK()

	kio.ReadStruct(cskC)

	Infof("reading from stream :: statistic stored in the file:")
	csk.Print()
	cskC.Check()
	csk.IsEqual(*cskC)

	Infof("reading from stream :: data is valid. success")
	
	Info("reading from stream :: sorting database")
	this.SortAct()
	Info("reading from stream :: database sorted")

	Info("reading from stream :: finished")

	return true
}




