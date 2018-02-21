// +build debug

package cmd

func Debugf(fmt string, args ...interface{}) {
	log.Debug(p.Sprintf(fmt, args...))
}
