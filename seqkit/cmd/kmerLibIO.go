package cmd

import (
	"github.com/shenwei356/xopen"
	"encoding/binary"
	"io"
)

type KmerIO struct {
	OutFh *xopen.Writer
	InFh *xopen.Reader
	mode int
	buf []byte
}

func (this *KmerIO) openWriter(outFile string) {
	outFh, err := xopen.Wopen(outFile)
	checkError(err)
	//defer outFh.Close()
    this.initWriter(outFh)
}

func (this *KmerIO) initWriter(outFh *xopen.Writer) {
	if this.mode != 0 {
		log.Panic("writing on open file")
	}
	this.buf        = make([]byte, binary.MaxVarintLen64)
	this.OutFh      = outFh
	this.mode       = 1
}



func (this *KmerIO) openReader(inFile string) {
	inFh, err := xopen.Ropen(inFile)
	checkError(err)
	//defer inFh.Close()
    this.initReader(inFh)
}

func (this *KmerIO) Flush() {
	this.CheckMode(1)

	this.OutFh.Flush()
}

func (this *KmerIO) Close() {
	if this.mode == 1 {
        this.Flush()
    	this.OutFh.Close()
    } else if this.mode == 2 {
    	this.InFh.Close()
    } else {
		log.Panic("closing already closed file")        
    }
}

func (this *KmerIO) initReader(inFh *xopen.Reader) {
	if this.mode != 0 {
		log.Panic("reading on open file")
	}
	this.buf        = make([]byte, binary.MaxVarintLen64)
	this.InFh       = inFh
	this.mode       = 2
}

func (this *KmerIO) CheckMode(mode int) {
	if mode == 1 {
		if this.mode == 0 {
			log.Panic("writing on closed file")
		}
		if this.mode == 2 {
			log.Panic("writing on reading file")
		}
	} else if mode == 2 {
		if this.mode == 0 {
			log.Panic("reading on closed file")
		}
		if this.mode == 1 {
			log.Panic("reading on reading file")
		}		
	}
}

func (this *KmerIO) ReadUint8(res *uint8) (bool) {
	this.CheckMode(2)
	
	err := binary.Read(this.InFh, binary.LittleEndian, res);

	if err != nil {
		if err == io.EOF {
			return false
		} else {
			log.Panic("binary.Read failed:", err)
		}
	}
	
	return true
}

func (this *KmerIO) ReadUint64(res *uint64) (bool) {
	this.CheckMode(2)
	
	err := binary.Read(this.InFh, binary.LittleEndian, res);

	if err != nil {
		if err == io.EOF {
			return false
		} else {
			log.Panic("binary.Read failed:", err)
		}
	}
	
	return true
}

func (this *KmerIO) ReadUint64V() (uint64, bool) {
	this.CheckMode(2)

	i, err := binary.ReadUvarint(this.InFh);

	if err != nil {
		if err == io.EOF {
			return 0, false
		} else {
			log.Panic("binary.Read failed:", err)
		}
	}
	
	return i, true
}

func (this *KmerIO) WriteUint8(x uint8) {
	this.CheckMode(1)

	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])

	err := binary.Write(this.OutFh, binary.LittleEndian, x)

	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteUint64(x uint64) {
	this.CheckMode(1)

	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])

	err := binary.Write(this.OutFh, binary.LittleEndian, x)

	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteUint64V(x uint64) {
	this.CheckMode(1)
	
	n := binary.PutUvarint(this.buf, x)
	
	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])
	
	err := binary.Write(this.OutFh, binary.LittleEndian, this.buf[:n])
	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteStruct(x interface{}) {
	this.CheckMode(1)
	err := binary.Write(this.OutFh, binary.LittleEndian, x)
	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}
func (this *KmerIO) ReadStruct(x interface{}) {
	this.CheckMode(2)
	err := binary.Read(this.InFh, binary.LittleEndian, x)
	if err != nil {
		log.Panic("binary.Write failed:", err)
	}
}

