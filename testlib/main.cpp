#include <unistd.h>  
#include <stdio.h>   
#include <string.h>
#include <iostream>
#include <vector>
#include <sys/time.h>

#include "librediscluster.h"
#include "biostream.h"

using namespace std;

#define LOG(os) std::cout<<"["<<__FILE__<<":"<<__FUNCTION__<<":"<<__LINE__<<"]"<<os<<std::endl;

const string paddrs = "127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381";
const int testnum = 100000;
LibRedisCluster *redis = NULL;

long getMS()
{
    struct timeval tv;
    gettimeofday(&tv,NULL);
	return tv.tv_sec*1000 + tv.tv_usec/1000;
}

void testBase();

int main()
{
	redis = new LibRedisCluster();
	redis->Connect(paddrs.c_str(),0);
	
	vector<string> _vKeys;
	vector<string> _vValues;
	_vKeys.reserve(testnum);
	_vValues.reserve(testnum);
	char str[200] = {0};
	
	long sPtart = getMS();
	for(int i =0;i<testnum;i++)
	{
		sprintf(str,"atest%d",i);
		//LOG("SETSTR="<<str);
		_vKeys.push_back(str);
		sprintf(str,"asdfasdasdksgasadsaqeqwwqrasfasdasdasdh%d",i);
		//LOG("GETSTR="<<str);
		_vValues.push_back(str);
	}
	long sPnd = getMS();
	LOG("init vector:start="<<sPtart<<",end="<<sPnd<<",time="<<sPnd-sPtart<<",keys="<<_vKeys.size()<<",vs="<<_vValues.size());
	
	LOG("");
	LOG("begin Set_String");
	long sStart = getMS();
	for(int i = 0;i<(int)_vKeys.size();i++)
	{
		redis->Set_String(_vKeys[i].c_str(),_vValues[i].c_str(),5);
	}
	long sEnd = getMS();
	LOG("SET_String:start="<<sStart<<",end="<<sEnd<<",time="<<sEnd-sStart);
	
	LOG("");
	LOG("begin Get_String");
	string get;
	long gStart = getMS();
	for(int i = 0;i<(int)_vKeys.size();i++)
	{
		redis->Get_String(_vKeys[i].c_str(),get);
	}
	long gEnd = getMS();
	LOG("Get_String:start="<<gStart<<",end="<<gEnd<<",time="<<gEnd-gStart);
	
	redis->Fini();
	delete redis;
	return 0;
}


void testBase()
{
	/********************************************************/
	bool ret = false;
	if(redis->Exists("testabc",ret))
	{
		LOG("testabc exists="<<(int)ret);
	}
	else
	{
		LOG("testabc not exists");
	}
	if(redis->Del("testabc"))
	{
		LOG("del testabc success");
	}
	else
	{
		LOG("del testabc err");
	}
	if(redis->Exists("testabc",ret))
	{
		LOG("testabc exists="<<(int)ret);
	}
	else
	{
		LOG("testabc not exists");
	}
	/********************************************************/
	LOG("\n");
	/********************************************************/
	if(redis->Set_String("teststring","123456",12312321))
	{
		LOG("set success");
	}
	else
	{
		LOG("set err");
	}
	string v;
	if(redis->Get_String("teststring",v))
	{
		LOG("get string="<<v)
	}
	else
	{
		LOG("can not get string");
	}
	//sleep(3);
	if(redis->Get_String("teststring",v))
	{
		LOG("get string="<<v)
	}
	else
	{
		LOG("can not get string");
	}
	/********************************************************/
	LOG("\n");
	/********************************************************/
	if(redis->HSet("htestkey",123456,sizeof(123456),"asfabafsa",sizeof("asfabafsa"),60))
	{
		LOG("hset success");
	}
	else
	{
		LOG("hset err");
	}
	char result[100];
	if(redis->HGet("htestkey",123456,sizeof(123456),result))
	{
		LOG("hget success,r="<<result);
	}
	else
	{
		LOG("hget err");
	}
	if(redis->HDel("htestkey",123456,sizeof(123456)))
	{
		LOG("hdel success");
	}
	else
	{
		LOG("hdel err");
	}
	if(redis->HGet("htestkey",123456,sizeof(123456),result))
	{
		LOG("hget success,r="<<result);
	}
	else
	{
		LOG("hget err");
	}
	/********************************************************/
}