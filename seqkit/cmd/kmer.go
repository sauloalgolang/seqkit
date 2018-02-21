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
	"github.com/spf13/cobra"
)

import (
	"github.com/shenwei356/go-logging"
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
		debug := getFlagBool(cmd, "debug")
		profile := getFlagBool(cmd, "profile")
		validateSeqLength := getFlagValidateSeqLength(cmd, "validate-seq-length")
		minLen := getFlagInt(cmd, "min-len")
		maxLen := getFlagInt(cmd, "max-len")
		kmerSize := int(getFlagPositiveInt(cmd, "kmer-size"))
		minCount := uint8(getFlagPositiveInt(cmd, "min-count"))
		
		if minLen >= 0 && maxLen >= 0 && minLen > maxLen {
			checkError(fmt.Errorf("value of flag -m (--min-len) should be >= value of flag -M (--max-len)"))
		}

		if kmerSize > 31 {
			checkError(fmt.Errorf("value of flag -k (--kmer-size) should be between 1 and 31"))
		}

		if minCount > 254 {
			checkError(fmt.Errorf("value of flag -c (--min-count) should be between 1 and 254"))
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
			Info("when flag -t (--seq-type) given, flag -v (--validate-seq) is automatically switched on")
			seq.ValidateSeq = true
		}

		files          := getFileList(args)
		
		var holder      = NewKmerHolder(kmerSize, minLen, maxLen, profile)

		//add := make(chan uint64, config.Threads*3);
		//var parser      = NewKmerParser(kmerSize, minLen, maxLen, func(v uint64){
		//	println("adding v", v, "max", config.Threads)
		//	add <- v
		//})
		

		Info( "threads ", config.Threads )

		//checkError(fmt.Errorf("done"))
		
		var err error
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
				
				if fastxReader.IsFastq {
					config.LineWidth = 0
					holder.ParseFastQ(file, "FQ", &record.Seq.Seq)
					//stats.AddSS(file, "FQ"       , parser.FastQ(&record.Seq.Seq))
				} else {
					Info("Parsing ", string(record.Name))
					holder.ParseFastA(file, record.Name, &record.Seq.Seq)
					//stats.AddSB(file, record.Name, parser.FastA(&record.Seq.Seq))
				}
			}
				
			config.LineWidth = lineWidth
		}

		holder.Wait()
		
		Info("Closing")
		holder.Close()
		//Infof("Printing")
		//holder.Print()
		
		Info("Generating Histogram")
		hist := NewHist()
		for i:=0; i < holder.NumKmers; i++ {
			kmer  := holder.GetByIndex(i)
			//Printf( " i: %12d kmer: %12d count: %3d seq: %s\n", i, kmer.Kmer, kmer.Count, converter.NumToSeq(kmer.Kmer));
			hist.Add(kmer.Count)
		}
		Infof("Printing Histogram")
		hist.Print()

		Infof("Printing Statistics")
		holder.PrintStats()
		
		Info("Saving to: ", outFile, "\n")

		holder.ToFile(outFile, minCount)

		Info("Finished saving\n")

		//outfh, err := xopen.Wopen(outFile)
		//checkError(err)
		//defer outfh.Close()
		//
		//kio := KmerIO{}
		//kio.initWriter(outfh)
		//holder.ToFileHandle(&kio, minCount)
		//outfh.Flush()
		//outfh.Close()

		Info("Reading from: ", outFile, "\n")
		holder.FromFile(outFile)
		Info("Finished reading\n")
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
	kmerCmd.Flags().IntP("kmer-size", "k", 5, "kmer size (1-31)")
	kmerCmd.Flags().IntP("min-count", "c", 1, "min kmer count to report (1-254)")
}
