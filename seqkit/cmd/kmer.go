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

import (
	"golang.org/x/text/message"
)

type Stat struct {
	Size      uint64
	Registers uint64
	Lines     uint64
	Chars     uint64
	Valids    uint64
	Counted   uint64
	Skipped   uint64
	Resets    uint64
}

type StatMap    map[string]*Stat
type StatMapMap map[string]StatMap





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
		kmerSize := uint64(getFlagPositiveInt(cmd, "kmer-size"))
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

		files              := getFileList(args)

		maxCount           := uint64((1 << uint(dbSize * 8))-1)

		p := message.NewPrinter(message.MatchLanguage("en"))
		
		println( "max count: ", maxCount )

		var val          uint64 = 0
		var lav          uint64 = 0
		var vav          uint64 = 0
		var cv           uint64 = 0
		var cw           uint64 = 0
		var ci           uint64 = 0
		var curr         uint64 = 0
		var seqLen       uint64 = 0

		var fileNames    []string = []string{}
		var seqNames     map[string][]string = map[string][]string{}
		var stats        Stat
		var statsFile    StatMap = StatMap{}
		var statsSeq     StatMapMap = StatMapMap{}

		var statsFileP  *Stat
		var statsSeqP   *Stat
		var res         = NewKmerHolder(kmerSize)
		
        var cleaner uint64  = (1 << (uint64(kmerSize)*2)) - 1

        vals               := [256][3]uint64{}
		CHARS              := [4]byte{'A', 'C', 'G', 'T'}
		chars              := [4]byte{'a', 'c', 'g', 't'}
        //res                := make([]uint8, cleaner)
		
		for _, a := range vals {
			for j, _ := range a {
				a[j] = 0
			}
		}

		for i, b := range CHARS {
			//print( "CHARS i: ", i, " b: ", b, "\n" );
			vals[uint8(b)][0] =    uint64(i)
			vals[uint8(b)][1] = (3-uint64(i)) << (2*(uint64(kmerSize)-1))
			vals[uint8(b)][2] = 1
		}

		for i, b := range chars {
			//print( "chars i: ", i, " b: ", b, "\n" );
			vals[uint8(b)][0] =    uint64(i)
			vals[uint8(b)][1] = (3-uint64(i)) << (2*(uint64(kmerSize)-1))
			vals[uint8(b)][2] = 1
		}

		//print( "cleaner ", cleaner, "\n")
		//print( "res     ",     res, "\n")

		//for j, b := range vals {
		//	//fmt.Printf( "vals i: %3d b: %3d (%010b)\n", i, b, b );
		//	v, w, i := b[0], b[1], b[2]
		//	fmt.Printf( "vals i: %3d v: %3d (%010b) w: %3d (%010b) i: %d\n", j, v, v, w, w, i );
		//}
		
		//checkError(fmt.Errorf("done"))
		
		var err error
		var sequence *seq.Seq
		var record *fastx.Record
		var fastxReader *fastx.Reader
		for _, file := range files {
			fastxReader, err = fastx.NewReader(alphabet, file, idRegexp)
			checkError(err)

			statsFile[file]            = &Stat{0,0,0,0,0,0,0,0}
			seqNames[file]             = []string{}
			fileNames = append(fileNames, file)

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
				
				sequence      = record.Seq

				seqLen        = uint64(len(sequence.Seq))
				
				if minLen >= 0 && seqLen < uint64(minLen) {
					continue
				}

				if maxLen >= 0 && seqLen > uint64(maxLen) {
					continue
				}
				
				statsFileP                 = statsFile[file]

				stats     .Size           += seqLen
				statsFileP.Size           += seqLen
				
				if ! fastxReader.IsFastq {
					p.Printf( "Parsing '%s' %12d\n", record.Name, seqLen )

					if statsSeq[file] == nil {
						statsSeq[file] = StatMap{}
					}

					seqNames[file] = append(seqNames[file], string(record.Name))

					statsSeq[file][string(record.Name)]            = &Stat{0,0,0,0,0,0,0,0}
					statsSeqP = statsSeq[file][string(record.Name)]
					
					stats     .Registers      += 1
					statsFileP.Registers      += 1
					statsSeqP .Size            = seqLen
				} else {
					stats     .Lines += 1
					statsFileP.Lines += 1
					config.LineWidth  = 0
				}
				
				val           = 0
				lav           = 0
				vav           = 0
				curr          = 0
				cv            = 0
				cw            = 0
				ci            = 0

				for _, b := range sequence.Seq {
					//fmt.Printf( "SEQ i: %v b: %v c: %c\n", i, b, b )

					stats     .Chars += 1
					statsFileP.Chars += 1

					if ! fastxReader.IsFastq {
						statsSeqP.Chars += 1
					}
					
					cv, cw, ci  = vals[ b ][0], vals[ b ][1], vals[ b ][2]
					
					//if count > 119200 {
					//fmt.Printf( "v       %12d - %010b - CHAR %s - CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", cv, cv, string(b), curr, count, valids, skipped, resets )
					//fmt.Printf( "w       %12d - %010b - CHAR %s - CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", cw, cw, " "      , curr, count, valids, skipped, resets )
					//}
					
					if ci == 0 {
						curr        = 0
						val         = 0
						lav         = 0
						vav         = 0

						stats     .Resets += 1
						statsFileP.Resets += 1
	
						if ! fastxReader.IsFastq {
							statsSeqP.Resets += 1
						}

						continue
				
					} else {
						stats     .Valids += 1
						statsFileP.Valids += 1
	
						if ! fastxReader.IsFastq {
							statsSeqP.Valids += 1
						}

						val       <<= 2
						val        &= cleaner
						val        += cv
				
						lav       >>= 2
						lav        += cw
						
						//if count > 119200 {
						//fmt.Printf( "val     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", val, val, curr, count, valids, skipped, resets )
						//fmt.Printf( "lav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", lav, lav, curr, count, valids, skipped, resets )
						//}
						
						if curr == kmerSize - 1 {
							vav = val
							
							if lav < val {
								vav = lav
							}
							
							res.Add(vav)
							stats     .Counted += 1
							statsFileP.Counted += 1

							if ! fastxReader.IsFastq {
								statsSeqP.Counted += 1
							}
								
							//if count > 119200 {
							//fmt.Printf( "vav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d RES %12d\n", vav, vav, curr, count, valids, skipped, resets, res[vav] )
							//}
						} else {
							//println(".", count)
							curr      += 1
						}
						//if count > 119200 {
						//println ()
						//}
					}
				}
			}
			config.LineWidth = lineWidth
		}

		var kcoun uint64 = 0
		var ksums uint64 = 0
		
		res.Close()
		
		for i:=0; i < res.KmerLen; i++ {
			kmer  := res.Get(i)
			//print( "RES i: ", i, " c: ", c, "\n" );
			kcoun += 1
			ksums += uint64(kmer.Count)
		}

		p.Printf( "Num Files      %12d\n", len(files) )
		p.Printf( "Num Kmers      %12d\n", ksums )
        p.Printf( "Num Uniq Kmers %12d\n", kcoun )

		//Size      uint64
		//Registers uint64
		//Lines     uint64
		//Chars     uint64
		//Valids    uint64
		//Skipped   uint64
		//Resets    uint64
		println("==========")

        p.Printf( "Size      %12d\n", stats.Size      )
        p.Printf( "Registers %12d\n", stats.Registers )
        p.Printf( "Lines     %12d\n", stats.Lines     )
        p.Printf( "Chars     %12d\n", stats.Chars     )
        p.Printf( "Valids    %12d\n", stats.Valids    )
        p.Printf( "Counted   %12d\n", stats.Counted   )
        p.Printf( "Skipped   %12d\n", stats.Skipped   )
        p.Printf( "Resets    %12d\n", stats.Resets    )

		println("==========")

		for _, filename := range fileNames {
			fStat    := statsFile[filename]
			seqStats := statsSeq[filename]

			println("  File: ", filename)

			p.Printf( "    Size      %12d\n", fStat.Size      )
			p.Printf( "    Registers %12d\n", fStat.Registers )
			p.Printf( "    Lines     %12d\n", fStat.Lines     )
			p.Printf( "    Chars     %12d\n", fStat.Chars     )
			p.Printf( "    Valids    %12d\n", fStat.Valids    )
			p.Printf( "    Counted   %12d\n", fStat.Counted   )
			p.Printf( "    Skipped   %12d\n", fStat.Skipped   )
			p.Printf( "    Resets    %12d\n", fStat.Resets    )

			println("  ----------")

			for _, seqName := range seqNames[filename] {
				fStat := seqStats[seqName]

				println("    Sequence: ", seqName)

				p.Printf( "      Size      %12d\n", fStat.Size      )
				p.Printf( "      Registers %12d\n", fStat.Registers )
				p.Printf( "      Lines     %12d\n", fStat.Lines     )
				p.Printf( "      Chars     %12d\n", fStat.Chars     )
				p.Printf( "      Valids    %12d\n", fStat.Valids    )
				p.Printf( "      Counted   %12d\n", fStat.Counted   )
				p.Printf( "      Skipped   %12d\n", fStat.Skipped   )
				p.Printf( "      Resets    %12d\n", fStat.Resets    )

				println("    **********")
			}
			println("  ----------")
		}
		println("==========")

		println("saving to: ", outFile, "\n")

		outfh, err := xopen.Wopen(outFile)
		checkError(err)
		defer outfh.Close()

		//outfh.Close()
		
		println("finished saving\n")
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
