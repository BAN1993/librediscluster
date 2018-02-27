MLIB = -L../librediscluster -lrediscluster -lhiredis_vip -levent
TARGET = tHirCluster

$(TARGET):main.o
	g++ -g -o $(TARGET)  main.o $(MLIB)

main.o:main.cpp
	g++ -g -c main.cpp

clean:
	rm main.o $(TARGET)
