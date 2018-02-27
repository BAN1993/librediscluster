/*********************************************/
// Supported redis cluster
// Based on hiredis-vip
//
// add by jxc 2017.06.10
/*********************************************/

#pragma once

#include <unistd.h>  
#include <stdio.h>  
#include <stdlib.h>  
#include <string.h>  
#include <signal.h>  
#include <event.h>
#include <iostream>

extern "C"
{
	#include <hiredis-vip/hircluster.h>
	#include <hiredis-vip/adapters/libevent.h>
}

#include "biostream.h"

#define LOGD(os) std::cout<<"["<<__FILE__<<":"<<__FUNCTION__<<":"<<__LINE__<<"]"<<os<<std::endl;
#define LOGI(os) std::cout<<"["<<__FILE__<<":"<<__FUNCTION__<<":"<<__LINE__<<"]"<<os<<std::endl;
#define LOGW(os) std::cout<<"["<<__FILE__<<":"<<__FUNCTION__<<":"<<__LINE__<<"]"<<os<<std::endl;
#define LOGE(os) std::cout<<"["<<__FILE__<<":"<<__FUNCTION__<<":"<<__LINE__<<"]"<<os<<std::endl;

const int LIBREDISCLUSTER_TIMEOUT = 5;

class LibRedisCluster
{
public:
	LibRedisCluster();
	~LibRedisCluster();
	
public:
//compatible interface
	bool Connect(const std::string& ip, const int port = 0/*no use*/, int timeout = LIBREDISCLUSTER_TIMEOUT);
	void Fini();
	bool DoKeepAlive();
	bool Exists(const std::string& key, bool& result);
	bool Del(const std::string& key);
	bool Set_String(const std::string& key, const std::string& value, time_t ex=_defaultex);
	bool Get_String(const std::string& key, std::string& value);
	
	template<class T, class P>
	bool HSet(const std::string& key, const T& filed, const int fsize, const P& value, const int vsize, time_t ex=_defaultex);
	template<class T, class P>
	bool HGet(const std::string& key, const T& filed, const int size, P& result);
	template<class T>
	bool HDel(const std::string& key, const T& filed, const int size);
	
public:
	bool setExpire(const std::string& key, time_t ex=_defaultex);
	
private:
	bool doCommand(redisReply** reply,const char* format,...);
	
private:
	redisClusterContext*	_redis;
	std::string 			_paddrs;
	int						_timeout;
	static time_t			_defaultex;
};



template<class T, class P>
bool LibRedisCluster::HSet(const std::string& key, const T& filed, const int fsize, const P& value, const int vsize, time_t ex)
{
	if(!_redis)
	{
		LOGE("redis is null");
		return false;
	}
	
	int fbsize = fsize + 100;
	char *fb = new char[fbsize];
	int vbsize = vsize + 100;
	char *vb = new char[vbsize];
	bool flag = false;
	redisReply* r = NULL;
	
	try
	{
		bostream fbos;
		fbos.attach(fb,fbsize);
		fbos<<filed;
		
		bostream vbos;
		vbos.attach(vb,vbsize);
		vbos<<value;
		
		if(!doCommand(&r,"hset %s %b %b",key.c_str(),fb,fbos.length(),vb,vbos.length()))
		{
			LOGE("doCommand err:fbsize="<<fbsize<<",fb="<<filed);
			LOGE("doCommand err:vbsize="<<vbsize<<",fb="<<value);
			flag = false;
		}
		else
		{
			if(!(r->type == REDIS_REPLY_INTEGER || r->type == REDIS_REPLY_STATUS))
			{
				LOGW("err,key="<<key);
				flag = false;
			}
			else
			{
				LOGD("success,key="<<key);
				flag = setExpire(key,ex);
			}
		}
		
	}
	catch(agproexception& e)
	{
		LOGE("agproexception:r="<<e.m_cause<<"fbsize="<<fbsize<<",fb="<<filed);
		LOGE("agproexception:r="<<e.m_cause<<"vbsize="<<vbsize<<",fb="<<value);
		flag = false;
	}
	catch(biosexception & e2)
	{
		LOGE("biosexception:r="<<e2.m_cause<<"fbsize="<<fbsize<<",fb="<<filed);
		LOGE("biosexception:r="<<e2.m_cause<<"vbsize="<<vbsize<<",fb="<<value);
		flag = false;
	}
	catch(...)
	{
		LOGE("unknown:fbsize="<<fbsize<<",fb="<<filed);
		LOGE("unknown:vbsize="<<vbsize<<",fb="<<value);
		flag = false;
	}
	
	delete []fb;
	delete []vb;
	freeReplyObject(r);
	return flag;
}

template<class T, class P>
bool LibRedisCluster::HGet(const std::string& key, const T& filed, const int size, P& result)
{
	if(!_redis)
	{
		LOGE("redis is null");
		return false;
	}
	
	int fsize = size+100;
	char* f = new char[fsize];
	bool flag = false;
	redisReply* r = NULL;
	
	try
	{
		bostream bos;
		bos.attach(f,fsize);
		bos<<filed;
		
		if(!doCommand(&r,"hget %s %b ",key.c_str(),f,bos.length()))
		{
			LOGE("doCommand err:fsize="<<fsize<<",fb="<<filed);
			flag = false;
		}
		else
		{
			if(!(r->type == REDIS_REPLY_STRING))
			{
				LOGW("err,key="<<key);
				flag = false;
			}
			else
			{
				bistream bis;
				bis.attach(r->str,r->len);
				bis>>result;
				flag = true;
			}
		}
		
	}
	catch(agproexception& e)
	{
		LOGE("agproexception:r="<<e.m_cause<<"fsize="<<fsize<<",fb="<<filed);
		flag = false;
	}
	catch(biosexception & e2)
	{
		LOGE("biosexception:r="<<e2.m_cause<<"fsize="<<fsize<<",fb="<<filed);
		flag = false;
	}
	catch(...)
	{
		LOGE("unknown:fsize="<<fsize<<",fb="<<filed);
		flag = false;
	}
	
	delete []f;
	freeReplyObject(r);
	return flag;
}

template<class T>
bool LibRedisCluster::HDel(const std::string& key, const T& filed, const int size)
{
	if(!_redis)
	{
		LOGE("redis is null");
		return false;
	}
	
	int fsize = size+100;
	char* f = new char[fsize];
	bool flag = false;
	redisReply* r = NULL;
	
	try
	{
		bostream bos;
		bos.attach(f,fsize);
		bos<<filed;
		
		if(!doCommand(&r,"hdel %s %b ",key.c_str(),f,bos.length()))
		{
			LOGE("doCommand err:fsize="<<fsize<<",fb="<<filed);
			flag = false;
		}
		else
		{
			if(!(r->type == REDIS_REPLY_INTEGER))
			{
				LOGW("err,key="<<key);
				flag = false;
			}
			else
			{
				flag = true;
			}
		}
		
	}
	catch(agproexception& e)
	{
		LOGE("agproexception:r="<<e.m_cause<<"fsize="<<fsize<<",fb="<<filed);
		flag = false;
	}
	catch(biosexception & e2)
	{
		LOGE("biosexception:r="<<e2.m_cause<<"fsize="<<fsize<<",fb="<<filed);
		flag = false;
	}
	catch(...)
	{
		LOGE("unknown:fsize="<<fsize<<",fb="<<filed);
		flag = false;
	}
	
	delete []f;
	freeReplyObject(r);
	return flag;
}







