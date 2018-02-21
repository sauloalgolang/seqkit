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

func IsLogLevelValid(lvl string) (r bool) {
	lvli, _ := logging.LogLevel(lvl)
	r = logging.GetLevel("seqkit") >= lvli
	return
}

func (b *StringBuffer) WriteStringf(fmt string, args ...interface{}) {
	b.WriteString(Sprintf(fmt, args...))
}

func PrintLevelf( lvln string, fmt string, args ...interface{} ) {
	lvli, _ := logging.LogLevel(lvln)
	
	switch; lvli {
		case logging.INFO:
			Infof(fmt, args...)
		case logging.NOTICE:
			Noticef(fmt, args...)
		case logging.WARNING:
			Warningf(fmt, args...)
		case logging.ERROR:
			Errorf(fmt, args...)
		case logging.CRITICAL:
			Criticalf(fmt, args...)
		case logging.DEBUG:
			Debugf(fmt, args...)
		default:
			Printf(fmt, args...)
	}
}

func PrintLevel( lvln string, msg ...interface{} ) {
	lvli, _ := logging.LogLevel(lvln)
	
	switch; lvli {
		case logging.INFO:
			Info(msg...)
		case logging.NOTICE:
			Notice(msg...)
		case logging.WARNING:
			Warning(msg...)
		case logging.ERROR:
			Error(msg...)
		case logging.CRITICAL:
			Critical(msg...)
		case logging.DEBUG:
			Debug(msg...)
		default:
			Print(msg...)
	}
}


func Infof(fmt string, args ...interface{}) {
	log.Info(p.Sprintf(fmt, args...))
}

func Info(msg ...interface{}) {
	log.Info(p.Sprint(msg...))
}



func Noticef(fmt string, args ...interface{}) {
	log.Notice(p.Sprintf(fmt, args...))
}

func Notice(msg ...interface{}) {
	log.Notice(p.Sprint(msg...))
}



func Warningf(fmt string, args ...interface{}) {
	log.Warning(p.Sprintf(fmt, args...))
}

func Warning(msg ...interface{}) {
	log.Warning(p.Sprint(msg...))
}



func Errorf(fmt string, args ...interface{}) {
	log.Error(p.Sprintf(fmt, args...))
}

func Error(msg ...interface{}) {
	log.Error(p.Sprint(msg...))
}



func Criticalf(fmt string, args ...interface{}) {
	log.Critical(p.Sprintf(fmt, args...))
}

func Critical(msg ...interface{}) {
	log.Critical(p.Sprint(msg...))
}



func Debug(msg ...interface{}) {
	log.Debug(p.Sprint(msg...))
}



func Panicf(fmt string, args ...interface{}) {
	log.Panic(p.Sprintf(fmt, args...))
}

func Panic(msg ...interface{}) {
	log.Panic(p.Sprint(msg...))
}



func Sprintf(fmt string, msg ...interface{}) string {
	return p.Sprintf(fmt, msg...)
}

func Printf(fmt string, args ...interface{}) {
	p.Printf(fmt, args...)
}

func Print(msg ...interface{}) {
	p.Print(msg...)
}