package cmd

import (
	"runtime"
	"github.com/shenwei356/go-logging"
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
		log.Info( "profiling" )
	}

	log.Info(p.Sprintf( "max db size %12d\n", max_kmer_size ))
	log.Info(p.Sprintf( "kmer size   %12d\n", k.KmerSize    ))
	log.Info(p.Sprintf( "kmer cap    %12d\n", k.KmerCap     ))
	
	return k
}


func (this *KmerHolder) Print() {	
	log.Debugf(p.Sprintf( "kmerSize     %12d\n", this.KmerSize  ))
	log.Debugf(p.Sprintf( "NumKmers     %12d\n", this.NumKmers  ))
	log.Debugf(p.Sprintf( "KmerCap      %12d\n", this.KmerCap   ))
	log.Debugf(p.Sprintf( "Kmer         %12d CAP %12d\n", len(this.Kmer), cap(this.Kmer) ))

	this.Kmer.Print()
}

func (this *KmerHolder) Add(kmer uint64) {
	this.mux.Lock()
	defer this.mux.Unlock()

	this.KmerCount++
	
	if this.KmerCount % 1000000 == 0 {
		tnow  := time.Now()
		tdiff := tnow.Sub(this.LastPrint)
		tinit := tnow.Sub(this.StartTIme)
		
		log.Info(p.Sprintf( "%12d %v %v\n", this.KmerCount, tinit, tdiff ))
		
		this.LastPrint      = time.Now()
	}
	
	this.Sort()
	this.Kmer.Add(kmer, this.LastNumKmers)
	this.NumKmers = len(this.Kmer)
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
		this.ReadStatsG.AddSB(key1, key2, *s)
	}()
}

func (this *KmerHolder) Wait() {
	log.Info("Waiting for conclusion")
	this.wg.Wait()
	log.Info("Finished reading")
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
		log.Info(p.Sprintf("KmerDb    :: Sort :: num kmers: %12d last kmer: %12d len kmer: %12d cap kmer: %12d", this.NumKmers, this.LastNumKmers, len(this.Kmer), cap(this.Kmer)))
	}
	
	//lvlD, _ := logging.LogLevel("DEBUG")
	lvlI, _ := logging.LogLevel("INFO" )
	
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Before :: %p Len %3d Cap %3d Prop %6.2f LastNumKmers %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastNumKmers)
	
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
		log.Info("KmerDb    :: Sort :: Add")

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

		//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Before")
		//this.Kmer.Print()

		sortSliceOffset(&this.Kmer, lenDst)
		
		//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: During")
		//this.Kmer.Print()
		
		for {
			if indexSrc == lenSrc { // no more buffer. stop
				//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: indexSrc (%03d) == lenSrc (%03d). breaking", indexSrc, lenSrc )
				break
			} else { // still has buffer
				//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: indexSrc (%03d) != lenSrc (%03d). merging", indexSrc, lenSrc )
				
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
				
				//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: dstIndex % 3d srcIndex % 3d",  indexDst,  indexSrc)
				//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: dstKmerK %03d srcKmerK %03d", *dstKmerK, *srcKmerK)

				if *dstKmerK == *srcKmerK { //same kmer
					//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Adding")

					//sum
					//*dstKmerC = sumInt8( *dstKmerC, *srcKmerC )
					addToInt8(dstKmerC, *srcKmerC)
					
					//next src
					indexSrc++
					
				} else if *dstKmerK < *srcKmerK { //db < buffer
					//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order")
					
					if indexDst >= lenDst {
						//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Swapping :: indexDst %03d LastNumKmers %03d", indexDst, this.LastNumKmers)
						//this.Kmer.Print()

						this.Kmer[indexDst], this.Kmer[indexSrc] = this.Kmer[indexSrc], this.Kmer[indexDst]
						//*dstKmer, *srcKmer = *srcKmer, *dstKmer

						//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d", indexSrc)
						//this.Kmer.Print()
						
						moveDownWhileSmall(&this.Kmer, indexSrc)
						
						//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
						//this.Kmer.Print()
						
						lenDst = indexDst + 1
						indexSrc++
					} else {
						//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Next Dst")
						
						//this.Kmer.Print()
							
						//next db
						indexDst++
					}
				} else if *dstKmerK > *srcKmerK { //db > buffer. worst case scnenario
					//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Swapping")

					//this.Kmer.Print()

					//swapping values
					this.Kmer[indexDst], this.Kmer[indexSrc] = this.Kmer[indexSrc], this.Kmer[indexDst]
					//*dstKmer, *srcKmer = *srcKmer, *dstKmer
					
					//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d", indexSrc)
					//this.Kmer.Print()
					
					moveDownWhileSmall(&this.Kmer, indexSrc)
					
					//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Swapping :: Move Down While Small :: indexSrc: %3d - Done", indexSrc)
					//this.Kmer.Print()
				}
			}
		}

		//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: After :: indexDst: (%03d)", indexDst)
		//this.Kmer.Print()

		if indexDst < lenDst {
			lasti = lenDst - 1
		} else {
			lasti = indexDst
		}
	}

	if lasti != len(this.Kmer) - 1 {
		//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Trimming :: last I %3d Len Buffer %3d", lasti, len(this.Kmer))
		this.Kmer = this.Kmer[:lasti+1]
	}
	
	//sumCountAfter := 0
	//for i,_ := range this.Kmer {
	//	sumCountAfter += int(this.Kmer[i].Count)
	//}
	
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Len Buffer Before %3d", lenBufferBefore   )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Last Buffer Len   %3d", this.LastNumKmers)
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Last I            %3d", lasti             )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Sum B             %3d", sumCountBefore    )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Sum A             %3d", sumCountAfter     )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Len               %3d", len(this.Kmer)  )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Cap               %3d", cap(this.Kmer)  )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Min K             %3d", minK              )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Max K             %3d", maxK              )
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: %p", this.Kmer)

	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Final")

	//this.Kmer.PrintLevel(lvlD)

	//if sumCountBefore != sumCountAfter {
	//	this.Kmer.PrintLevel(lvlI)
	//	log.Debugf("sum differs")
	//}
	
	if len(this.Kmer) >= ( 4 * (cap(this.Kmer) / 5)) {
		newCap := (cap(this.Kmer) / 4 * 6)

		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: Before :: Len     %d", len(this.Kmer)))
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: Before :: Cap     %d", cap(this.Kmer)))
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: Before :: New Cap %d", newCap))
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: Before :: Address %p", this.Kmer))
		
		t := make(KmerDb, len(this.Kmer), newCap)
		copy(t, this.Kmer)
		this.Kmer = t
		
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: After  :: Len     %d", len(this.Kmer)))
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: After  :: Cap     %d", cap(this.Kmer)))
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: After  :: New Cap %d", newCap))
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: After  :: Address %p", this.Kmer))
		
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: Running GC"))
		runtime.GC()
		log.Info(p.Sprintf("KmerDb    :: Sort :: Extend :: GC Run"))
	}
	
	//this.Kmer.Print()

	if len(this.Kmer) < this.LastNumKmers {
		this.Kmer.PrintLevel(lvlI)
		log.Panic("BUFFER REDUCED SIZE")
	}
	
	this.NumKmers     = len(this.Kmer)
	this.LastNumKmers = len(this.Kmer)
	log.Info("KmerDb    :: Sort :: Sorted")
	log.Info(p.Sprintf("KmerDb    :: Sort :: num kmers: %12d last kmer: %12d len kmer: %12d cap kmer: %12d", this.NumKmers, this.LastNumKmers, len(this.Kmer), cap(this.Kmer)))
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: After  :: %p Len %3d Cap %3d Prop %6.2f LastNumKmers %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastNumKmers)
	//this.Kmer.PrintLevel(lvlI)
}









func (this *KmerHolder) ToFile(outFile string, minCount uint8) bool {
	kio := NewKmerIO()
	kio.openWriter(outFile)
	//defer kio.Flush()
	defer kio.Close()
	return this.ToFileHandle(kio, minCount)
}

func (this *KmerHolder) ToFileHandle(kio *KmerIO, minCount uint8) bool {
	log.Info("saving to stream")

	log.Info("saving to stream :: sorting database")
	this.SortAct()
	log.Info("saving to stream :: database sorted")

	
	var kmer     uint64 = 0
	var count    uint8  = 0
	var lastKmer uint64 = 0
	var kmerdiff uint64 = 0
	
	var numK     uint64 = 0
	csk := NewChecksumK()

	var regs     uint64 = uint64(len(this.Kmer))
	log.Info(p.Sprintf("saving to stream :: writing %23d registers\n"    , regs    ))
	log.Info(p.Sprintf("saving to stream :: writing %23d minimun count\n", minCount))

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
				log.Panicf(p.Sprintf("duplicated kmer. %d vs %d", kmer, lastKmer))
			}
			if kmerdiff == 0 {
				log.Panicf(p.Sprintf("zero difference kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff))
			}
		}

		if count == 0 {
			log.Panicf(p.Sprintf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff))
		}
		
		csk.Add(kmer, count, kmerdiff)
				
		//fmt.Printf("W k %d kmer %d count %d kmerdiff %d\n", k, kmer, count, kmerdiff)
		//kio.WriteUint64(kmerdiff)
		kio.WriteUint64V(kmerdiff)
		kio.WriteUint8(count)
		lastKmer = kmer
	}

	log.Info(p.Sprintf("saving to stream :: registers written: %12d ", numK ))
	
	csk.Print()

	if numK != regs {
		log.Panicf(p.Sprintf("number of writen registers not the same as expected. %d vs %d", numK, regs))
	}

	csk.Check()

	log.Info(p.Sprint("saving to stream :: data is valid. writing statistics"))

	kio.WriteStruct(csk)
	
	log.Info(p.Sprint("saving to stream :: finished"))

	return true
}

func (this *KmerHolder) FromFile(inFile string) bool {
	kio := KmerIO{}
	kio.openReader(inFile)
	defer kio.Close()
	return this.FromFileHandle(&kio)
}

func (this *KmerHolder) FromFileHandle(kio *KmerIO) bool {
	log.Info("reading from stream")

	log.Info("reading from stream :: cleaning database")
	this.Clear()
	log.Info("reading from stream :: database clean")
	
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
	if !succes { log.Panic("error reading begining of the file") }
	
	succes = kio.ReadUint8(&minCount)
	if !succes { log.Panic("error reading begining of the file") }
	
	
	
	log.Info(p.Sprintf("reading from stream :: reading %20d registers\n", regs))
	log.Info(p.Sprintf("reading from stream :: reading %20d minimum count\n", minCount))
	
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
					log.Panic(p.Sprintf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff))
				}
			}
		}
		
		if count == 0 {
			log.Panic(p.Sprintf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff))
		}

		kmer          = lastKmer + kmerdiff

		csk.Add(kmer, count, kmerdiff)

		this.AddSorted(kmer, count)
		
		//fmt.Printf("R k %d kmer %d count %d kmerdiff %d\n", numK, kmer, count, kmerdiff)
		
		lastKmer = kmer
		
		if numK == regs {
			break
		}
	}

	log.Info(p.Sprintf("reading from stream :: registers read: %12d ", numK))
	
	log.Info(p.Sprintf("reading from stream :: statistic of data acquired:"))
	csk.Print()

	if numK != regs { log.Panic(p.Sprintf("number of read registers not the same as expected. %d vs %d", numK, regs)) }

	csk.Check()


	log.Info(p.Sprintf("reading from stream :: reading statistics in file"))

	cskC := NewChecksumK()

	kio.ReadStruct(cskC)

	log.Info(p.Sprintf("reading from stream :: statistic stored in the file:"))
	csk.Print()
	cskC.Check()
	csk.IsEqual(*cskC)

	log.Info(p.Sprintf("reading from stream :: data is valid. success"))
	
	log.Info("reading from stream :: sorting database")
	this.SortAct()
	log.Info("reading from stream :: database sorted")

	log.Info("reading from stream :: finished")

	return true
}




