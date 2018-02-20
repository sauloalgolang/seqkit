package cmd

//https://dave.cheney.net/2014/09/28/using-build-to-switch-between-debug-and-release

import (
	"github.com/shenwei356/go-logging"
	"bytes"
)

//lvlD, _ := logging.LogLevel("DEBUG")
//lvlI, _ := logging.LogLevel("INFO" )

//lvlI, _ := logging.LogLevel("INFO")
//this.Kmer.PrintLevel(lvlI)

//this.Kmer.PrintLevel(lvlD)

//if sumCountBefore != sumCountAfter {
//	this.Kmer.PrintLevel(lvlI)
//	Debugf("sum differs")
//}


//func Print() {
//	lvl, _ := logging.LogLevel("DEBUG")
//	this.PrintLevel(lvl)
//}
//
//func PrintLevel(lvl logging.Level) {
//	if logging.GetLevel("seqkit") >= lvl {
//		for i,j := range *this {
//			//log.Debugf(p.Sprintf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count ))
//			Printf( "  %12d :: %12d -> %3d\n", i, j.Kmer, j.Count )
//		}
//	}
//}


type StringBuffer struct {
	bytes.Buffer
}

func (b *StringBuffer) WriteStringf(fmt string, args ...interface{}) {
	b.WriteString(Sprintf(fmt, args))
}


func IsLogLevelValid(lvl string) (r bool) {
	lvli, _ := logging.LogLevel(lvl)
	r = logging.GetLevel("seqkit") >= lvli
	return
}

func PrintLevelf( lvln string, fmt string, args ...interface{} ) {
	lvli, _ := logging.LogLevel(lvln)
	
	switch; lvli {
		case logging.INFO:
			Info(fmt, args)
		case logging.NOTICE:
			Notice(fmt, args)
		case logging.WARNING:
			Warning(fmt, args)
		case logging.ERROR:
			Error(fmt, args)
		case logging.CRITICAL:
			Critical(fmt, args)
		case logging.DEBUG:
			Debug(fmt, args)
		default:
			Printf(fmt, args)
	}

}



func Infof(fmt string, args ...interface{}) {
	log.Info(p.Sprintf(fmt, args))
}

func Info(msg ...interface{}) {
	log.Info(msg)
}



func Noticef(fmt string, args ...interface{}) {
	log.Notice(p.Sprintf(fmt, args))
}

func Notice(msg ...interface{}) {
	log.Notice(msg)
}



func Warningf(fmt string, args ...interface{}) {
	log.Warning(p.Sprintf(fmt, args))
}

func Warning(msg ...interface{}) {
	log.Warning(msg)
}



func Errorf(fmt string, args ...interface{}) {
	log.Error(p.Sprintf(fmt, args))
}

func Error(msg ...interface{}) {
	log.Error(msg)
}



func Criticalf(fmt string, args ...interface{}) {
	log.Critical(p.Sprintf(fmt, args))
}

func Critical(msg ...interface{}) {
	log.Critical(msg)
}



func Debug(msg ...interface{}) {
	log.Debug(msg)
}



func Panicf(fmt string, args ...interface{}) {
	log.Panic(p.Sprintf(fmt, args))
}

func Panic(msg ...interface{}) {
	log.Panic(msg)
}



func Sprintf(msg ...interface{}) string {
	return p.Sprintf(msg)
}

func Printf(fmt string, args ...interface{}) {
	p.Printf(fmt, args)
}