package cmd

type Hist struct {
	h [254] int64
	i bool
}

func (this *Hist) Update( prev uint8, next uint8 ) {
	if ! this.i {
		this.Init()
	}
	this.h[prev]--
	this.h[next]++
}

func (this *Hist) Add( val uint8 ) {
	if ! this.i {
		this.Init()
	}
	this.h[val]++
}

func (this *Hist) Init() {	
	this.h = [254]int64{}
	this.i = true
}

func (this *Hist) Clear() {	
	if this.i {
		this.h = [254]int64{}
	}
	this.Init()
}
