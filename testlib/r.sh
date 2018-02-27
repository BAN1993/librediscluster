ulimit -c unlimited
rm -rf core.*
make clean 1>/dev/null 2>/dev/null
make
echo ""
redis-cli -c set testabc aaahhh
echo ""
./tHirCluster
