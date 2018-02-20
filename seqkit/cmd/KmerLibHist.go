package cmd

type Hist struct {
	Unique   uint64
	Total    uint64
	Hist   []uint64
}

func NewHist() (h *Hist) {
	h = new(Hist)
	h.Clear()
	return
}

func (this *Hist) Add( val uint8 ) {
	this.Unique++
	this.Total += uint64(val)
	//Info(val)
	this.Hist[val]++
}

func (this *Hist) Clear() {	
	this.Unique = 0
	this.Total  = 0
	this.Hist   = make([]uint64, 255, 255)
}

func (this Hist) String() string {
	var buffer StringBuffer

	buffer.WriteStringf("Unique Kmers: %12d\n", this.Unique)
	buffer.WriteStringf("Total  Kmers: %12d\n", this.Total )

	i := len(this.Hist)-1
	for ; i >= 0; i-- {
		if this.Hist[i] != 0 {
			break
		}
	}
	
	i++
	
	for j:=1; j<i; j++ {
		buffer.WriteStringf(" %03d %12d\n", j, this.Hist[j] )
	}
	
	return buffer.String()
}

func (this Hist) Print() {
	Infof("\n%v", this)
}
