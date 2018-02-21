package cmd

type StatMap    map[string]*Stat
type StatMapMap map[string]StatMap

type Stat struct {
	Size      int
	Sequences int
	Chars     int
	Resets    int
	Valids    int
	Counted   int
	Skipped   int
}

func NewStat() (s *Stat) {
	s           = new(Stat)
	s.Size      = 0
	s.Sequences = 0
	s.Chars     = 0
	s.Resets    = 0
	s.Valids    = 0
	s.Counted   = 0
	s.Skipped   = 0
	return
}

func (this *Stat) Sum( that Stat ) {
	this.Size      += that.Size
	this.Sequences += that.Sequences
	this.Chars     += that.Chars
	this.Resets    += that.Resets
	this.Valids    += that.Valids
	this.Counted   += that.Counted
	this.Skipped   += that.Skipped
}

func (this Stat) String() string {
	var buffer StringBuffer
	
	buffer.WriteStringf("  Size     : %12d\n", this.Size     )
	buffer.WriteStringf("  Sequences: %12d\n", this.Sequences)
	buffer.WriteStringf("  Chars    : %12d\n", this.Chars    )
	buffer.WriteStringf("  Resets   : %12d\n", this.Resets   )
	buffer.WriteStringf("  Valids   : %12d\n", this.Valids   )
	buffer.WriteStringf("  Counted  : %12d\n", this.Counted  )
	buffer.WriteStringf("  Skipped  : %12d\n", this.Skipped  )
	
	return buffer.String()
}

func (this Stat) Print() {
	Infof("%v", this)
}








type KmerReadStat struct {
	Key1 []string                    // ordered keys lvl1
	Key2 map[string][]string         // ordered keys lvl2 grouped by lvl1
	Dict map[string]map[string]*Stat // key1 key2 stats
}

func NewKmerReadStat() (k *KmerReadStat) {
	k      = new(KmerReadStat)
	k.Key1 = []string{}
	k.Key2 = make(map[string][]string)
	k.Dict = make(map[string]map[string]*Stat)

	return
}

func (this *KmerReadStat) AddSS(key1 string, key2 string, val Stat) { this.add(       key1 ,        key2 , val) } //String String
func (this *KmerReadStat) AddSB(key1 string, key2 []byte, val Stat) { this.add(       key1 , string(key2), val) } //String Byte
func (this *KmerReadStat) AddBS(key1 []byte, key2 string, val Stat) { this.add(string(key1),        key2 , val) } //Byte   String
func (this *KmerReadStat) AddBB(key1 []byte, key2 []byte, val Stat) { this.add(string(key1), string(key2), val) } //Byte   Byte

func (this *KmerReadStat) Add(key1 interface{}, key2 interface{}, val Stat) { // Universal
	var k1, k2 string = "", ""
	
	switch key1 := key1.(type) {
		case string:
			k1 = key1
		case []byte:
			k1 = string(key1)
		default:
			Panic("unknown key format")
	}
	
	switch key2 := key2.(type) {
		case string:
			k2 = key2
		case []byte:
			k2 = string(key2)
		default:
			Panic("unknown key format")
	}
	
	 this.add(k1, k2, val)
}

func (this *KmerReadStat) add(key1, key2 string, val Stat) {
	_,ok1 := this.Dict[key1]

	if ! ok1 {
		this.Dict[key1] = make(map[string]*Stat)
		this.Key2[key1] = []string{}
		this.Key1       = append(this.Key1, key1)
	}

	_,ok2 := this.Dict[key1][key2]

	if ! ok2 {
		this.Dict[key1][key2] = NewStat()
		this.Key2[key1]       = append(this.Key2[key1], key2)
	}

	this.Dict[key1][key2].Sum(val)
}


func (this *KmerReadStat) Merge(that *KmerReadStat) {
	for _, key1 := range that.Key1 {
		_,ok1 := this.Dict[key1]

		if ! ok1 {
			this.Dict[key1] = make(map[string]*Stat)
			this.Key2[key1] = []string{}
			this.Key1       = append(this.Key1, key1)
		}

		for _, key2 := range that.Key2[key1] {
			_,ok2 := this.Dict[key1][key2]
			if ! ok2 {
				this.Dict[key1][key2] = NewStat()
				this.Key2[key1]       = append(this.Key2[key1], key2)
			}

			this.Dict[key1][key2].Sum(*that.Dict[key1][key2])
		}
	}
}

func (this KmerReadStat) String() string {
	var buffer StringBuffer
	
	statAll     := NewStat()
	numAllFiles := len(this.Key1)
	numAllSeqs  := 0

	for _, fileName := range this.Key1 {
		statFile    := NewStat()
		
		for _, seqName := range this.Key2[fileName] {
			buffer.WriteStringf("File Name %s Seq Name %s\n", fileName, seqName)
			buffer.WriteStringf("%v", *this.Dict[fileName][seqName])
			
			statFile.Sum(*this.Dict[fileName][seqName])
			statAll .Sum(*this.Dict[fileName][seqName])
		}
		
		numSeqsFile := len(this.Key2[fileName])
		numAllSeqs  += numSeqsFile

		buffer.WriteStringf("File Name %s :: %12d Files\n", fileName, numSeqsFile)
		buffer.WriteStringf("%v", statFile)
	}

	
	buffer.WriteStringf("All :: %12d Files :: %12d Sequences\n", numAllFiles, numAllSeqs)
	buffer.WriteStringf("%v", statAll)
	
	return buffer.String()
}

func (this KmerReadStat) Print() {
	Infof("\n%v", this)
}