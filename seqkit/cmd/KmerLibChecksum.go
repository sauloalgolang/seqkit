package cmd

import (
	"bytes"
)

type ChecksumK struct {
	NumK     uint64
	MinK     uint64
	MaxK     uint64
	MinC     uint8
	MaxC     uint8
	MinD     uint64
	MaxD     uint64
	SumC     uint64
	SumD     uint64
}

func NewChecksumK() (csk *ChecksumK) {
	csk        = new(ChecksumK)

	csk.NumK   = 0
	csk.MinK   = MaxUint
	csk.MaxK   = 0
	csk.MinC   = 254
	csk.MaxC   = 0
	csk.MinD   = MaxUint
	csk.MaxD   = 0
	csk.SumC   = 0
	csk.SumD   = 0

	return
}

func (this *ChecksumK) Add(kmer uint64, count uint8, kmerdiff uint64) {
	this.NumK++
	
	if kmer     < this.MinK { this.MinK = kmer     }
	if kmer     > this.MaxK { this.MaxK = kmer     }
	if count    < this.MinC { this.MinC = count    }
	if count    > this.MaxC { this.MaxC = count    }
	if kmerdiff < this.MinD { this.MinD = kmerdiff }
	if kmerdiff > this.MaxD { this.MaxD = kmerdiff }
	
	this.SumC += uint64(count)
	this.SumD += kmerdiff
}

func (this ChecksumK) Check() {
	if (this.MaxK - this.MinK) != this.SumD {
		log.Panic(p.Sprintf("Total range of kmers is invalid. Min %d Max %d Diff %d SumDiff %d"   , this.MaxK, this.MinK, (this.MaxK - this.MinK), this.SumD))
	}
}

func (this ChecksumK) IsEqual(that ChecksumK) {
	if this.NumK != that.NumK { log.Panic(p.Sprintf("number of kmer not the same as expected. %d vs %d"   , this.NumK, that.NumK)) }
	if this.MinK != that.MinK { log.Panic(p.Sprintf("minimal kmer not the same as expected. %d vs %d"     , this.MinK, that.MinK)) }
	if this.MaxK != that.MaxK { log.Panic(p.Sprintf("maximum kmer not the same as expected. %d vs %d"     , this.MaxK, that.MaxK)) }
	if this.MinC != that.MinC { log.Panic(p.Sprintf("minimal count not the same as expected. %d vs %d"    , this.MinC, that.MinC)) }
	if this.MaxC != that.MaxC { log.Panic(p.Sprintf("maximum count not the same as expected. %d vs %d"    , this.MaxC, that.MaxC)) }
	if this.MinD != that.MinD { log.Panic(p.Sprintf("minimal kmer diff not the same as expected. %d vs %d", this.MinD, that.MinD)) }
	if this.MaxD != that.MaxD { log.Panic(p.Sprintf("maximum kmer diff not the same as expected. %d vs %d", this.MaxD, that.MaxD)) }
	if this.SumC != that.SumC { log.Panic(p.Sprintf("sum of counts not the same as expected. %d vs %d"    , this.SumC, that.SumC)) }
	if this.SumD != that.SumD { log.Panic(p.Sprintf("sum of diff not the same as expected. %d vs %d"      , this.SumD, that.SumD)) }
}

func (this ChecksumK) String() string {
	var buffer bytes.Buffer

	buffer.WriteString(p.Sprintf("Kmer  Valid: %12d\n", this.NumK ))
	buffer.WriteString(p.Sprintf("Kmer  Min  : %12d\n", this.MinK ))
	buffer.WriteString(p.Sprintf("Kmer  Max  : %12d\n", this.MaxK ))
	buffer.WriteString(p.Sprintf(""))
	buffer.WriteString(p.Sprintf("Diff  Sum  : %12d\n", this.SumD ))
	buffer.WriteString(p.Sprintf("Diff  Min  : %12d\n", this.MinD ))
	buffer.WriteString(p.Sprintf("Diff  Max  : %12d\n", this.MaxD ))
	buffer.WriteString(p.Sprintf(""))
	buffer.WriteString(p.Sprintf("Count Sum  : %12d\n", this.SumC ))
	buffer.WriteString(p.Sprintf("Count Min  : %12d\n", this.MinC ))
	buffer.WriteString(p.Sprintf("Count Max  : %12d\n", this.MaxC ))

	return buffer.String()
}

func (this *ChecksumK) Print() {
	log.Info(p.Sprintf("\n%v", this))
}
