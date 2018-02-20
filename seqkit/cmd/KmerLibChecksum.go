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
		Panicf("Total range of kmers is invalid. Min %d Max %d Diff %d SumDiff %d"   , this.MaxK, this.MinK, (this.MaxK - this.MinK), this.SumD)
	}
}

func (this ChecksumK) IsEqual(that ChecksumK) {
	if this.NumK != that.NumK { Panicf("number of kmer not the same as expected. %d vs %d"   , this.NumK, that.NumK) }
	if this.MinK != that.MinK { Panicf("minimal kmer not the same as expected. %d vs %d"     , this.MinK, that.MinK) }
	if this.MaxK != that.MaxK { Panicf("maximum kmer not the same as expected. %d vs %d"     , this.MaxK, that.MaxK) }
	if this.MinC != that.MinC { Panicf("minimal count not the same as expected. %d vs %d"    , this.MinC, that.MinC) }
	if this.MaxC != that.MaxC { Panicf("maximum count not the same as expected. %d vs %d"    , this.MaxC, that.MaxC) }
	if this.MinD != that.MinD { Panicf("minimal kmer diff not the same as expected. %d vs %d", this.MinD, that.MinD) }
	if this.MaxD != that.MaxD { Panicf("maximum kmer diff not the same as expected. %d vs %d", this.MaxD, that.MaxD) }
	if this.SumC != that.SumC { Panicf("sum of counts not the same as expected. %d vs %d"    , this.SumC, that.SumC) }
	if this.SumD != that.SumD { Panicf("sum of diff not the same as expected. %d vs %d"      , this.SumD, that.SumD) }
}

func (this ChecksumK) String() string {
	var buffer bytes.Buffer

	buffer.WriteString(Sprintf("Kmer  Valid: %12d\n", this.NumK ))
	buffer.WriteString(Sprintf("Kmer  Min  : %12d\n", this.MinK ))
	buffer.WriteString(Sprintf("Kmer  Max  : %12d\n", this.MaxK ))
	buffer.WriteString(Sprintf(""))
	buffer.WriteString(Sprintf("Diff  Sum  : %12d\n", this.SumD ))
	buffer.WriteString(Sprintf("Diff  Min  : %12d\n", this.MinD ))
	buffer.WriteString(Sprintf("Diff  Max  : %12d\n", this.MaxD ))
	buffer.WriteString(Sprintf(""))
	buffer.WriteString(Sprintf("Count Sum  : %12d\n", this.SumC ))
	buffer.WriteString(Sprintf("Count Min  : %12d\n", this.MinC ))
	buffer.WriteString(Sprintf("Count Max  : %12d\n", this.MaxC ))

	return buffer.String()
}

func (this *ChecksumK) Print() {
	Infof("\n%v", this)
}
