// Copyright © 2016 Wei Shen <shenwei356@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"fmt"
	"io"
	"runtime"

	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/xopen"
	"github.com/spf13/cobra"
)

// kmerCmd represents the seq command
var kmerCmd = &cobra.Command{
	Use:   "kmer",
	Short: "Count kmers",
	Long: `Count kmers

`,
	Run: func(cmd *cobra.Command, args []string) {
		config := getConfigs(cmd)
		alphabet := config.Alphabet
		idRegexp := config.IDRegexp
		lineWidth := config.LineWidth
		outFile := config.OutFile
		seq.AlphabetGuessSeqLenghtThreshold = config.AlphabetGuessSeqLength
		runtime.GOMAXPROCS(config.Threads)

		validateSeq := getFlagBool(cmd, "validate-seq")
		validateSeqLength := getFlagValidateSeqLength(cmd, "validate-seq-length")
		minLen := getFlagInt(cmd, "min-len")
		maxLen := getFlagInt(cmd, "max-len")
		kmerSize := getFlagPositiveInt(cmd, "kmer-size")
		dbSize := getFlagPositiveInt(cmd, "db-size")

		if minLen >= 0 && maxLen >= 0 && minLen > maxLen {
			checkError(fmt.Errorf("value of flag -m (--min-len) should be >= value of flag -M (--max-len)"))
		}

		if kmerSize > 31 {
			checkError(fmt.Errorf("value of flag -k (--kmer-size) should be between 1 and 31"))
		}

		if dbSize > 8 {
			checkError(fmt.Errorf("value of flag -d (--db-size) should be between 1 and 8"))
		}
		
		seq.ValidateSeq = validateSeq
		seq.ValidateWholeSeq = false
		seq.ValidSeqLengthThreshold = validateSeqLength
		seq.ValidSeqThreads = config.Threads
		seq.ComplementThreads = config.Threads

		if !(alphabet == nil || alphabet == seq.Unlimit) {
			log.Info("when flag -t (--seq-type) given, flag -v (--validate-seq) is automatically switched on")
			seq.ValidateSeq = true
		}

		files := getFileList(args)

		
		
        vals               := make([]int64, 256, 256)
		CHARS              := [4]byte{'A', 'C', 'G', 'T'}
		chars              := [4]byte{'a', 'c', 'g', 't'}
        var cleaner uint64  = (1 << (uint64(kmerSize)*2)) - 1
        res                := make([]uint8, cleaner)

		var val uint64      = 0
		var lav uint64      = 0
		var cv   int64      = 0
		var cw   int64      = 0
		curr               := 0
		count              := 0
		
		for i, _ := range vals {
			vals[i] = -1
		}

		for i, b := range CHARS {
			//print( "CHARS i: ", i, " b: ", b, "\n" );
			vals[uint8(b)] = int64(i)
		}

		for i, b := range chars {
			//print( "chars i: ", i, " b: ", b, "\n" );
			vals[uint8(b)] = int64(i)
		}

		print( "cleaner ", cleaner, "\n")
		print( "res     ",     res, "\n")

		//for i, b := range vals {
		//	print( "vals i: ", i, " b: ", b, "\n" );
		//}
		
		//checkError(fmt.Errorf("done"))
		
		outfh, err := xopen.Wopen(outFile)
		checkError(err)
		defer outfh.Close()
		var sequence *seq.Seq
		var record *fastx.Record
		var fastxReader *fastx.Reader
		for _, file := range files {
			fastxReader, err = fastx.NewReader(alphabet, file, idRegexp)
			checkError(err)

			for {
				record, err = fastxReader.Read()

				if err != nil {
					if err == io.EOF {
						break
					}
					checkError(err)
					break
				}

				ab := fastxReader.Alphabet()
				if ab != seq.DNA && ab != seq.DNAredundant {
					print( ab )
					checkError(fmt.Errorf("Not a DNA sequence"))
				}
				
				if minLen >= 0 && len(record.Seq.Seq) < minLen {
					continue
				}

				if maxLen >= 0 && len(record.Seq.Seq) > maxLen {
					continue
				}

				if fastxReader.IsFastq {
					config.LineWidth = 0
				} else {
					fmt.Printf( "Parsing %s\n", record.Name )
				}

				sequence = record.Seq

				val      = 0
				lav      = 0
				curr     = 0
				count    = 0
				cv       = 0
				cw       = 0
					
				for _, b := range sequence.Seq {
					//fmt.Printf( "SEQ i: %v b: %v c: %c\n", i, b, b )

					count += 1
					cv     = vals[ b ]
					
					if cv == -1 {
						curr = 0
						val  = 0
						lav  = 0
						continue
					} else {
						cw      = 3 - cv
						//# print( "v       {} {:12,d} - {} - CHAR {} - CURR {} COUNT {:12d}".format(" "*22, v, toBin(v), chr(s), curr, count ) )
						//# print( "w       {} {:12,d} - {} - CHAR {} - CURR {} COUNT {:12d}".format(" "*22, w, toBin(w), " "   , curr, count ) )
						val <<= 2
						val  &= cleaner
						val  += uint64(cv)
						lav >>= 2
						lav  += uint64(cw) << (2*(uint64(kmerSize)-1))
						//# print( "val     {} {:12,d} - {}".format(" "*22, val, toBin(val) ) )
						//# print( "lav     {} {:12,d} - {}".format(" "*22, lav, toBin(lav) ) )
						if curr == kmerSize - 1 {
						//	# print( "val     {} {:12,d} - {}".format(" "*22, val, toBin(val) ) )
						//	# print( "lav     {} {:12,d} - {}".format(" "*22, lav, toBin(lav) ) )
						//	# print()
							if lav < val {
								if res[lav] < 254 {
									res[lav] += 1
								}
							} else {
								if res[val] < 254 {
									res[val] += 1
								}
							}
						} else {
						//	# print(".")
							curr += 1
						}
					}
				}
			}
			config.LineWidth = lineWidth
		}

		var kcoun uint64 = 0
		var ksums uint64 = 0
		
		for _, c := range res {
			//print( "RES i: ", i, " c: ", c, "\n" );
			if c > 0 {
				kcoun += 1
				ksums += uint64(c)
			}
		}

		fmt.Printf( "Num Kmers      %v\n", ksums )
        fmt.Printf( "Num Uniq Kmers %v\n", kcoun )

		outfh.Close()
	},
}


func init() {
	RootCmd.AddCommand(kmerCmd)

	kmerCmd.Flags().BoolP("validate-seq", "v", false, "validate bases according to the alphabet")
	kmerCmd.Flags().IntP("validate-seq-length", "V", 10000, "length of sequence to validate (0 for whole seq)")
	kmerCmd.Flags().IntP("min-len", "m", -1, "only print sequences longer than the minimum length (-1 for no limit)")
	kmerCmd.Flags().IntP("max-len", "M", -1, "only print sequences shorter than the maximum length (-1 for no limit)")
	kmerCmd.Flags().IntP("kmer-size", "k", 5, "kmer size (1-31, default: 21)")
	kmerCmd.Flags().IntP("db-size", "d", 1, "database size (1-8, default: 1)")

}
