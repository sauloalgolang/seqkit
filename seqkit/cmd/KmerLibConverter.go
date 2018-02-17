package cmd

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
		











