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
	"github.com/shenwei356/go-logging"
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
		debug := getFlagBool(cmd, "debug")
		profile := getFlagBool(cmd, "profile")
		validateSeqLength := getFlagValidateSeqLength(cmd, "validate-seq-length")
		minLen := getFlagInt(cmd, "min-len")
		maxLen := getFlagInt(cmd, "max-len")
		kmerSize := int(getFlagPositiveInt(cmd, "kmer-size"))
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

		if config.Quiet && debug {
			checkError(fmt.Errorf("Cannot be quiet (--quiet) and debug (-d) at the same time"))			
		}
		
		if debug {
			logging.SetLevel(logging.DEBUG, "seqkit")
		} else if config.Quiet {
			logging.SetLevel(logging.ERROR, "seqkit")
		} else {
			logging.SetLevel(logging.INFO, "seqkit")
		}
		
		
		if !(alphabet == nil || alphabet == seq.Unlimit) {
			log.Info("when flag -t (--seq-type) given, flag -v (--validate-seq) is automatically switched on")
			seq.ValidateSeq = true
		}

		files              := getFileList(args)

		maxCount           := uint64((1 << uint(dbSize * 8))-1)

		p := message.NewPrinter(message.MatchLanguage("en"))
		
		if profile {
			log.Info( "profile" )
		}		
		
		log.Info( "max count: ", maxCount )

		var val          uint64 = 0
		var lav          uint64 = 0
		var vav          uint64 = 0
		var cv           uint64 = 0
		var cw           uint64 = 0
		var ci           uint64 = 0
		var curr          int   = 0
		var seqLen       uint64 = 0

		var fileNames    []string = []string{}
		var seqNames     map[string][]string = map[string][]string{}
		var stats        Stat
		var statsFile    StatMap = StatMap{}
		var statsSeq     StatMapMap = StatMapMap{}

		var statsFileP  *Stat
		var statsSeqP   *Stat
		var res         = NewKmerHolder(kmerSize)
		
		converter := NewConverter(kmerSize)
		
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
				
				if fastxReader.IsFastq {
					stats     .Lines += 1
					statsFileP.Lines += 1
					config.LineWidth  = 0
					
					if profile && statsFileP.Lines == 100 {
						break
					}
				} else {
					log.Infof(p.Sprintf( "Parsing '%s' %12d\n", record.Name, seqLen ))

					if statsSeq[file] == nil {
						statsSeq[file] = StatMap{}
					}

					seqNames[file] = append(seqNames[file], string(record.Name))

					statsSeq[file][string(record.Name)]            = &Stat{0,0,0,0,0,0,0,0}
					statsSeqP = statsSeq[file][string(record.Name)]
					
					stats     .Registers      += 1
					statsFileP.Registers      += 1
					statsSeqP .Size            = seqLen
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
						if profile && statsSeqP.Chars == 10000 {
							break
						}
					}
					
					cv, cw, ci  = converter.Vals[ b ][0], converter.Vals[ b ][1], converter.Vals[ b ][2]
					
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
						val        &= converter.Cleaner
						val        += cv
				
						lav       >>= 2
						lav        += cw
						
						//if count > 119200 {
						//fmt.Printf( "val     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", val, val, curr, count, valids, skipped, resets )
						//fmt.Printf( "lav     %12d - %010b            CURR %d COUNT %12d VALIDS %12d SKIPPED %12d RESETS %12d\n", lav, lav, curr, count, valids, skipped, resets )
						//}
						
						if curr == converter.KmerSize - 1 {
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
							//log.Info(".", count)
							curr      += 1
						}
						//if count > 119200 {
						//log.Info ()
						//}
					}
				}
			}
			config.LineWidth = lineWidth
		}

		var kcoun uint64 = 0
		var ksums uint64 = 0
		
		log.Infof("Closing")
		res.Close()
		//log.Infof("Printing")
		//res.Print()
		print( "num kmers", res.KmerLen )
		
		for i:=0; i < res.KmerLen; i++ {
			kmer  := res.GetByIndex(i)
			fmt.Printf( " i: %12d kmer: %12d count: %3d seq: %s\n", i, kmer.Kmer, kmer.Count, converter.NumToSeq(kmer.Kmer));
			kcoun += 1
			ksums += uint64(kmer.Count)
		}

		log.Infof(p.Sprintf( "Num Files      %12d\n", len(files) ))
		log.Infof(p.Sprintf( "Num Kmers      %12d\n", ksums      ))
        log.Infof(p.Sprintf( "Num Uniq Kmers %12d\n", kcoun      ))

		//Size      uint64
		//Registers uint64
		//Lines     uint64
		//Chars     uint64
		//Valids    uint64
		//Skipped   uint64
		//Resets    uint64
		log.Info("==========")

        log.Infof(p.Sprintf( "Size      %12d\n", stats.Size      ))
        log.Infof(p.Sprintf( "Registers %12d\n", stats.Registers ))
        log.Infof(p.Sprintf( "Lines     %12d\n", stats.Lines     ))
        log.Infof(p.Sprintf( "Chars     %12d\n", stats.Chars     ))
        log.Infof(p.Sprintf( "Valids    %12d\n", stats.Valids    ))
        log.Infof(p.Sprintf( "Counted   %12d\n", stats.Counted   ))
        log.Infof(p.Sprintf( "Skipped   %12d\n", stats.Skipped   ))
        log.Infof(p.Sprintf( "Resets    %12d\n", stats.Resets    ))

		log.Info("==========")

		for _, filename := range fileNames {
			fStat    := statsFile[filename]
			seqStats := statsSeq[filename]

			log.Info("  File: ", filename)

			log.Infof(p.Sprintf( "    Size      %12d\n", fStat.Size      ))
			log.Infof(p.Sprintf( "    Registers %12d\n", fStat.Registers ))
			log.Infof(p.Sprintf( "    Lines     %12d\n", fStat.Lines     ))
			log.Infof(p.Sprintf( "    Chars     %12d\n", fStat.Chars     ))
			log.Infof(p.Sprintf( "    Valids    %12d\n", fStat.Valids    ))
			log.Infof(p.Sprintf( "    Counted   %12d\n", fStat.Counted   ))
			log.Infof(p.Sprintf( "    Skipped   %12d\n", fStat.Skipped   ))
			log.Infof(p.Sprintf( "    Resets    %12d\n", fStat.Resets    ))

			log.Info("  ----------")

			for _, seqName := range seqNames[filename] {
				fStat := seqStats[seqName]

				log.Info("    Sequence: ", seqName)

				log.Infof(p.Sprintf( "      Size      %12d\n", fStat.Size      ))
				log.Infof(p.Sprintf( "      Registers %12d\n", fStat.Registers ))
				log.Infof(p.Sprintf( "      Lines     %12d\n", fStat.Lines     ))
				log.Infof(p.Sprintf( "      Chars     %12d\n", fStat.Chars     ))
				log.Infof(p.Sprintf( "      Valids    %12d\n", fStat.Valids    ))
				log.Infof(p.Sprintf( "      Counted   %12d\n", fStat.Counted   ))
				log.Infof(p.Sprintf( "      Skipped   %12d\n", fStat.Skipped   ))
				log.Infof(p.Sprintf( "      Resets    %12d\n", fStat.Resets    ))

				log.Info("    **********")
			}
			log.Info("  ----------")
		}
		log.Info("==========")

		log.Info("saving to: ", outFile, "\n")

		outfh, err := xopen.Wopen(outFile)
		checkError(err)
		defer outfh.Close()

		//outfh.Close()
		
		log.Info("finished saving\n")
	},
}


func init() {
	RootCmd.AddCommand(kmerCmd)

	kmerCmd.Flags().BoolP("validate-seq", "v", false, "validate bases according to the alphabet")
	kmerCmd.Flags().BoolP("debug", "b", false, "debug")
	kmerCmd.Flags().BoolP("profile", "p", false, "profile")
	kmerCmd.Flags().IntP("validate-seq-length", "V", 10000, "length of sequence to validate (0 for whole seq)")
	kmerCmd.Flags().IntP("min-len", "m", -1, "only print sequences longer than the minimum length (-1 for no limit)")
	kmerCmd.Flags().IntP("max-len", "M", -1, "only print sequences shorter than the maximum length (-1 for no limit)")
	kmerCmd.Flags().IntP("kmer-size", "k", 5, "kmer size (1-31, default: 21)")
	kmerCmd.Flags().IntP("db-size", "d", 1, "database size (1-8, default: 1)")

}
