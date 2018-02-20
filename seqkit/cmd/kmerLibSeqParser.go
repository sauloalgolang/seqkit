package cmd

type FORMAT int

const (
	FASTA FORMAT = iota
	FASTQ
)

type AdderFunc func(uint64, uint8)

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

	add       AdderFunc

	Converter *Converter
}

func NewKmerParser(kmerSize, minLen, maxLen int, profile bool, add AdderFunc) (*KmerParser){
	c := NewConverter(kmerSize)

    k := KmerParser{}
	k.val       = 0
	k.lav       = 0
	k.vav       = 0

	k.cv        = 0
	k.cw        = 0
	k.ci        = 0

	k.curr      = 0

	k.minLen    = minLen
	k.maxLen    = maxLen

	k.Profile   = profile

	k.add       = add

	k.Converter = c

    return &k
}

func (this *KmerParser) FastQ(seq *[]byte) (s *Stat) {
	s = this.fast(seq, FASTQ)
	return
}

func (this *KmerParser) FastA(seq *[]byte) (s *Stat) {
	s = this.fast(seq, FASTA)
	return
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
        //Infof( "SEQ i: %v b: %v c: %c\n", i, b, b )

        s.Chars += 1

		if this.Profile && s.Chars == 10000 {
			break
		}
        
        this.cv, this.cw, this.ci  = this.Converter.Vals[ b ][0], this.Converter.Vals[ b ][1], this.Converter.Vals[ b ][2]
        
        //if count > 119200 {
        //Infof( "v       %12d - %010b - CHAR %s - CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", cv, cv, string(b), curr, count, valids, skipped, resets )
        //Infof( "w       %12d - %010b - CHAR %s - CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", cw, cw, " "      , curr, count, valids, skipped, resets )
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
            //Infof( "val     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", val, val, curr, count, valids, skipped, resets )
            //Infof( "lav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", lav, lav, curr, count, valids, skipped, resets )
            //}
            
            if this.curr == this.Converter.KmerSize - 1 {
                this.vav = this.val
                
                if this.lav < this.val {
                    this.vav = this.lav
                }
                
                this.add(this.vav, 1)
                s.Counted += 1
                    
                //if count > 119200 {
                //Infof( "vav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d RES %12d\n", vav, vav, curr, count, valids, skipped, resets, res[vav] )
                //}
            } else {
                //Info(".", count)
                this.curr += 1
                s.Skipped += 1
           }
            //if count > 119200 {
            //Info ()
            //}
        }
    }
	
	return
}
