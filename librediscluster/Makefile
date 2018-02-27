INC = -I/usr/local/include
MLIB = -L/usr/local/lib -lhiredis_vip -levent
LIB_OBJ = librediscluster.o
LIB_OUT = librediscluster.a

CFLAGS+=-g

lib:$(LIB_OBJ)
	ar -rv $(LIB_OUT) $(LIB_OBJ) 
	
clean:
	rm -rf *.o $(LIB_OUT) $(LIB_OBJ)
