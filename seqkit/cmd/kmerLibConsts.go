package cmd

import (
	"golang.org/x/text/message"
)

const min_capacity =  1000
const max_capacity = 10000
const max_counter  = uint8(254)
//https://stackoverflow.com/questions/6878590/the-maximum-value-for-an-int-type-in-go
const MaxUint      = ^uint64(0) 
const MinUint      = 0
const MaxInt       = int64(MaxUint >> 1)
const MinInt       = -MaxInt - 1

var p = message.NewPrinter(message.MatchLanguage("en"))

type FORMAT int

const (
	FASTA FORMAT = iota
	FASTQ
)
