package cmd

import (
	"sync"
)

type FORMAT int

const (
	FASTA FORMAT = iota
	FASTQ
)

type AdderFunc func(uint64)

type KmerParser struct {
	val        uint64
	lav        uint64
	vav        uint64
	
	cv         uint64
	cw         uint64
	ci         uint64
	
	curr        int
	
	minLen      int
	maxLen      int
	
	Profile    bool
	
	Add        AdderFunc

	Converter *Converter
	wg         sync.WaitGroup
}

func NewKmerParser(kmerSize, minLen, maxLen int, add AdderFunc) (*KmerParser){
	c := NewConverter(kmerSize)

    k := KmerParser{0,0,0, 0,0,0, 0, minLen, maxLen, false, add, c, sync.WaitGroup{}}

    return &k
}

func (this *KmerParser) Wait() {
	log.Info("Waiting for conclusion")
	this.wg.Wait()
	log.Info("Finished reading")
}

func (this *KmerParser) FastQ(seq *[]byte, fs func (*Stat)) {
	this.wg.Add(1)
	//println("wg.Addding")
	go func() {
		defer this.wg.Done()
		s := this.fast(seq, FASTQ)
		fs( s )
		//println("wg.Done")
	}()
}

func (this *KmerParser) FastA(seq *[]byte, fs func (*Stat)) {
	this.wg.Add(1)
	//println("wg.Addding")
	go func() {
		defer this.wg.Done()
		s := this.fast(seq, FASTA)
		fs( s )
		//println("wg.Done")
	}()
}

func (this *KmerParser) fast(seq *[]byte, fmt FORMAT) (s *Stat) {
    this.val  = 0
    this.lav  = 0
    this.vav  = 0

    this.cv   = 0
    this.cw   = 0
    this.ci   = 0

    this.curr = 0

	s         = NewStat()

	s.Size   += len(*seq)
	
	if this.minLen >= 0 && s.Size < this.minLen {
		return
	}
	
	if this.maxLen >= 0 && s.Size > this.maxLen {
		return
	}

	s.Sequences += 1

    for _, b := range (*seq) {
        //fmt.Printf( "SEQ i: %v b: %v c: %c\n", i, b, b )

        s.Chars += 1

		if this.Profile && s.Chars == 10000 {
			break
		}
        
        this.cv, this.cw, this.ci  = this.Converter.Vals[ b ][0], this.Converter.Vals[ b ][1], this.Converter.Vals[ b ][2]
        
        //if count > 119200 {
        //fmt.Printf( "v       %12d - %010b - CHAR %s - CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", cv, cv, string(b), curr, count, valids, skipped, resets )
        //fmt.Printf( "w       %12d - %010b - CHAR %s - CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", cw, cw, " "      , curr, count, valids, skipped, resets )
        //}
        
        if this.ci == 0 {
            this.curr = 0
            this.val  = 0
            this.lav  = 0
            this.vav  = 0

            s.Resets += 1

            continue
    
        } else {
            this.val <<= 2
            this.val  &= this.Converter.Cleaner
            this.val  += this.cv
    
            this.lav >>= 2
            this.lav  += this.cw
            
            s.Valids  += 1

            //if count > 119200 {
            //fmt.Printf( "val     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", val, val, curr, count, valids, skipped, resets )
            //fmt.Printf( "lav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", lav, lav, curr, count, valids, skipped, resets )
            //}
            
            if this.curr == this.Converter.KmerSize - 1 {
                this.vav = this.val
                
                if this.lav < this.val {
                    this.vav = this.lav
                }
                
                this.Add(this.vav)
                s.Counted += 1
                    
                //if count > 119200 {
                //fmt.Printf( "vav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d RES %12d\n", vav, vav, curr, count, valids, skipped, resets, res[vav] )
                //}
            } else {
                //log.Info(".", count)
                this.curr += 1
                s.Skipped += 1
           }
            //if count > 119200 {
            //log.Info ()
            //}
        }
    }
	
	return
}
