package cmd

import (
	"github.com/shenwei356/xopen"
	"encoding/binary"
	"io"
)

type FILEMODE int

const (
	CLOSED FILEMODE = iota
	READ
	WRITE
)

type KmerIO struct {
	InFh  *xopen.Reader
	OutFh *xopen.Writer
	mode  FILEMODE
	buf   []byte
}

func NewKmerIO() (k *KmerIO) {
	k      = new(KmerIO)
	k.mode = CLOSED
	k.buf  = make([]byte, binary.MaxVarintLen64)
	return
}

func (this *KmerIO) initReader(inFh *xopen.Reader) {
	if this.mode != CLOSED {
		Panic("reading on open file")
	}
	this.buf        = make([]byte, binary.MaxVarintLen64)
	this.InFh       = inFh
	this.mode       = READ
}

func (this *KmerIO) initWriter(outFh *xopen.Writer) {
	if this.mode != CLOSED {
		Panic("writing on open file")
	}
	this.buf        = make([]byte, binary.MaxVarintLen64)
	this.OutFh      = outFh
	this.mode       = WRITE
}




func (this *KmerIO) openReader(inFile string) {
	inFh, err := xopen.Ropen(inFile)
	checkError(err)
	//defer inFh.Close()
    this.initReader(inFh)
}

func (this *KmerIO) openWriter(outFile string) {
	outFh, err := xopen.Wopen(outFile)
	checkError(err)
	//defer outFh.Close()
    this.initWriter(outFh)
}



func (this *KmerIO) CheckMode(mode FILEMODE) {
	if mode == WRITE {
		if this.mode == CLOSED {
			Panic("writing on closed file")
		}
		if this.mode == READ {
			Panic("writing on reading file")
		}
	} else if mode == READ {
		if this.mode == CLOSED {
			Panic("reading on closed file")
		}
		if this.mode == WRITE {
			Panic("reading on writing file")
		}
	}
}

func (this *KmerIO) Flush() {
	this.CheckMode(WRITE)

	this.OutFh.Flush()
}

func (this *KmerIO) Close() {
	if this.mode == WRITE {
        this.Flush()
    	this.OutFh.Close()
    } else if this.mode == READ {
    	this.InFh.Close()
    } else {
		Panic("closing already closed file")        
    }
}






func (this *KmerIO) ReadUint8(res *uint8) (bool) {
	this.CheckMode(READ)
	
	err := binary.Read(this.InFh, binary.LittleEndian, res);

	if err != nil {
		if err == io.EOF {
			return false
		} else {
			Panic("binary.Read failed:", err)
		}
	}
	
	return true
}

func (this *KmerIO) ReadUint64(res *uint64) (bool) {
	this.CheckMode(READ)
	
	err := binary.Read(this.InFh, binary.LittleEndian, res);

	if err != nil {
		if err == io.EOF {
			return false
		} else {
			Panic("binary.Read failed:", err)
		}
	}
	
	return true
}

func (this *KmerIO) ReadUint64V() (uint64, bool) {
	this.CheckMode(READ)

	i, err := binary.ReadUvarint(this.InFh);

	if err != nil {
		if err == io.EOF {
			return 0, false
		} else {
			Panic("binary.Read failed:", err)
		}
	}
	
	return i, true
}




func (this *KmerIO) WriteUint8(x uint8) {
	this.CheckMode(WRITE)

	//Printf("%d %d %x\n", x, n, this.buf[:n])

	err := binary.Write(this.OutFh, binary.LittleEndian, x)

	if err != nil {
		Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteUint64(x uint64) {
	this.CheckMode(WRITE)

	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])

	err := binary.Write(this.OutFh, binary.LittleEndian, x)

	if err != nil {
		Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) WriteUint64V(x uint64) {
	this.CheckMode(WRITE)
	
	n := binary.PutUvarint(this.buf, x)
	
	//fmt.Printf("%d %d %x\n", x, n, this.buf[:n])
	
	err := binary.Write(this.OutFh, binary.LittleEndian, this.buf[:n])
	if err != nil {
		Panic("binary.Write failed:", err)
	}
}







func (this *KmerIO) WriteStruct(x interface{}) {
	this.CheckMode(WRITE)
	err := binary.Write(this.OutFh, binary.LittleEndian, x)
	if err != nil {
		Panic("binary.Write failed:", err)
	}
}

func (this *KmerIO) ReadStruct(x interface{}) {
	this.CheckMode(READ)
	err := binary.Read(this.InFh, binary.LittleEndian, x)
	if err != nil {
		Panic("binary.Write failed:", err)
	}
}
