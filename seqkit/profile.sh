export CPUPROFILE=pprof
export MEMPROFILE=mprof

./seqkit kmer -p ../../../S_lycopersicum_chromosomes.2.50.fa.gz

echo run go tool pprof ./seqkit pprof
echo run go tool pprof --alloc_space ./seqkit mprof
