package cmd

import (
	"fmt"
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

func (this *ChecksumK) Print() {
	fmt.Printf("valid: %12d :: kmer min: %12d max: %12d :: diff sum: %12d min: %12d max: %12d :: count sum: %12d min: %12d max: %12d\n",
			   this.NumK, this.MinK, this.MaxK, this.SumD, this.MinD, this.MaxD, this.SumC, this.MinC, this.MaxC )
}

func (this *ChecksumK) IsEqual(that *ChecksumK) {
	if this.NumK != that.NumK { log.Panicf("number of kmer not the same as expected. %d vs %d"   , this.NumK, that.NumK) }
	if this.MinK != that.MinK { log.Panicf("minimal kmer not the same as expected. %d vs %d"     , this.MinK, that.MinK) }
	if this.MaxK != that.MaxK { log.Panicf("maximum kmer not the same as expected. %d vs %d"     , this.MaxK, that.MaxK) }
	if this.MinC != that.MinC { log.Panicf("minimal count not the same as expected. %d vs %d"    , this.MinC, that.MinC) }
	if this.MaxC != that.MaxC { log.Panicf("maximum count not the same as expected. %d vs %d"    , this.MaxC, that.MaxC) }
	if this.MinD != that.MinD { log.Panicf("minimal kmer diff not the same as expected. %d vs %d", this.MinD, that.MinD) }
	if this.MaxD != that.MaxD { log.Panicf("maximum kmer diff not the same as expected. %d vs %d", this.MaxD, that.MaxD) }
	if this.SumC != that.SumC { log.Panicf("sum of counts not the same as expected. %d vs %d"    , this.SumC, that.SumC) }
	if this.SumD != that.SumD { log.Panicf("sum of diff not the same as expected. %d vs %d"      , this.SumD, that.SumD) }
}
			   
func NewChecksumK() *ChecksumK {
	csk       := ChecksumK{}

	csk.NumK   = 0
	csk.MinK   = MaxUint
	csk.MaxK   = 0
	csk.MinC   = 254
	csk.MaxC   = 0
	csk.MinD   = MaxUint
	csk.MaxD   = 0
	csk.SumC   = 0
	csk.SumD   = 0
	
	return &csk
}


