package cmd

import (
	"runtime"
	"fmt"
	"golang.org/x/text/message"
	"github.com/shenwei356/go-logging"
)

const min_capacity = 1000000
const max_counter  = uint8(254)
//https://stackoverflow.com/questions/6878590/the-maximum-value-for-an-int-type-in-go
const MaxUint      = ^uint64(0) 
const MinUint      = 0
const MaxInt       = int64(MaxUint >> 1)
const MinInt       = -MaxInt - 1

var p = message.NewPrinter(message.MatchLanguage("en"))



type KmerHolder struct {
	KmerSize     int
	KmerLen      int
	KmerCap      int
	LastKmerLen  int
	hist         Hist
	Kmer         KmerDb
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
	
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Before :: %p Len %3d Cap %3d Prop %6.2f LastKmerLen %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastKmerLen)
	
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
	
	if this.LastKmerLen == 0 {
		// first adding
		log.Infof("KmerDb    :: Sort :: All")

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
		indexSrc := this.LastKmerLen
	
		lenDst   := this.LastKmerLen
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
						//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Out of order :: Swapping :: indexDst %03d LastKmerLen %03d", indexDst, this.LastKmerLen)
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
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: Last Buffer Len   %3d", this.LastKmerLen)
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

		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: Before :: Len     %d", len(this.Kmer))
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: Before :: Cap     %d", cap(this.Kmer))
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: Before :: New Cap %d", newCap)
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: Before :: Address %p", this.Kmer)
		
		t := make(KmerDb, len(this.Kmer), newCap)
		copy(t, this.Kmer)
		this.Kmer = t
		
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: After  :: Len     %d", len(this.Kmer))
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: After  :: Cap     %d", cap(this.Kmer))
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: After  :: New Cap %d", newCap)
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: After  :: Address %p", this.Kmer)
		
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: Running GC")
		runtime.GC()
		log.Infof("KmerDb    :: Merge :: Merge & Sort :: Extend :: GC Run")
	}
	
	//this.Kmer.Print()

	if len(this.Kmer) < this.LastKmerLen {
		this.Kmer.PrintLevel(lvlI)
		log.Panicf("BUFFER REDUCED SIZE")
	}
	
	this.KmerLen     = len(this.Kmer)
	this.LastKmerLen = len(this.Kmer)
	println("Sorted")
	//log.Debugf("KmerDb    :: Merge :: Merge & Sort :: After  :: %p Len %3d Cap %3d Prop %6.2f LastKmerLen %3d", this.Kmer, len(this.Kmer), cap(this.Kmer), float64(len(this.Kmer)) / float64(cap(this.Kmer)) * 100.0, this.LastKmerLen)
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

func (this *KmerHolder) AddSorted(kmer uint64, count uint8) {
	this.Kmer.AddSorted(kmer, count)
	this.KmerLen = len(this.Kmer)
}

func (this *KmerHolder) Clear() {
	this.Kmer.Clear()
	this.KmerLen = len(this.Kmer)
	this.LastKmerLen = len(this.Kmer)
}

func (this *KmerHolder) ToFile(outFile string, minCount uint8) bool {
	kio := KmerIO{}
	kio.openWriter(outFile)
	//defer kio.Flush()
	defer kio.Close()
	return this.ToFileHandle(&kio, minCount)
}

func (this *KmerHolder) ToFileHandle(kio *KmerIO, minCount uint8) bool {
	println("saving to stream")
	
	var kmer     uint64 = 0
	var count    uint8  = 0
	var lastKmer uint64 = 0
	var kmerdiff uint64 = 0
	
	var numK     uint64 = 0
	csk := NewChecksumK()

	var regs     uint64 = uint64(len(this.Kmer))
	fmt.Printf("writing %12d registers\n", regs)
	fmt.Printf("writing %12d minimun count\n", minCount)

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
				log.Panicf("duplicated kmer. %d vs %d", kmer, lastKmer)
			}
			if kmerdiff == 0 {
				log.Panicf("zero difference kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
			}
		}

		if count == 0 {
			log.Panicf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
		}
		
		csk.Add(kmer, count, kmerdiff)
				
		//fmt.Printf("W k %d kmer %d count %d kmerdiff %d\n", k, kmer, count, kmerdiff)
		//kio.WriteUint64(kmerdiff)
		kio.WriteUint64V(kmerdiff)
		kio.WriteUint8(count)
		lastKmer = kmer
	}

	fmt.Printf("WRITE registers: %12d ", numK )
	
	csk.Print()

	kio.WriteStruct(csk)

	if numK != regs {
		log.Panicf("number of writen registers not the same as expected. %d vs %d", numK, regs)
	}

	//kio.Flush()
	//kio.Close()
	
	return true
}

func (this *KmerHolder) FromFile(inFile string) bool {
	kio := KmerIO{}
	kio.openReader(inFile)
	defer kio.Close()
	return this.FromFileHandle(&kio)
}

func (this *KmerHolder) FromFileHandle(kio *KmerIO) bool {
	println("reading from stream")

	println("cleaning database")
	this.Clear()
	println("database clean")
	
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
	
	fmt.Printf("reading %12d registers\n", regs)
	fmt.Printf("reading %12d minimum count\n", minCount)
	
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
					log.Panicf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
				}
			}
		}
		
		if count == 0 {
			log.Panicf("zero count kmer %12d count %3d lastKmer %12d kmerdiff %12d", kmer, count, lastKmer, kmerdiff)
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

	fmt.Printf("READ  registers: %12d ", numK)
	csk.Print()
	
	if numK != regs { log.Panicf("number of read registers not the same as expected. %d vs %d", numK, regs) }

	cskC := NewChecksumK()

	kio.ReadStruct(cskC)

	csk.IsEqual(cskC)

	println("sorting database")
	this.SortAct()
	println("database sorted")
		
	return true
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
	    k.Kmer           = make(KmerDb, 0, k.KmerCap  )

	log.Infof(p.Sprintf( "max db size %12d\n", max_kmer_size ))
	log.Infof(p.Sprintf( "kmer size   %12d\n", k.KmerSize    ))
	log.Infof(p.Sprintf( "kmer cap    %12d\n", k.KmerCap     ))
	
	return &k
}
