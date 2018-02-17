export CPUPROFILE=pprof
export MEMPROFILE=mprof

time ./seqkit kmer -k11 -p -o /tmp/t.kmer.gz ../../../S_lycopersicum_chromosomes.2.50.fa.gz

echo help: https://blog.golang.org/profiling-go-programs
echo run go tool pprof ./seqkit pprof
echo run go tool pprof --alloc_space ./seqkit mprof
