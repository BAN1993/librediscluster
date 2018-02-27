#include "librediscluster.h"

time_t LibRedisCluster::_defaultex = 86400; // one day

LibRedisCluster::LibRedisCluster():_redis(NULL)
{
	_paddrs = "";
	_timeout = 0;
}

LibRedisCluster::~LibRedisCluster()
{
	if(_redis)
	{
		delete _redis;
	}
}

bool LibRedisCluster::Connect(const std::string& ip, const int port, int timeout)
{
	try
	{
		_paddrs = ip;
		_timeout = timeout;
		struct timeval tv;
		tv.tv_sec = timeout;
		tv.tv_usec = 0;
		
		_redis = redisClusterConnectWithTimeout(ip.c_str(), tv, REDIS_BLOCK);
		
		if(_redis->err)
		{
			LOGE("connect err,addrs="<<ip<<",errtype="<<_redis->err<<",errstr="<<_redis->errstr);
			return false;
		}
	}
	catch (...)
	{
		LOGE("connect err,addrs="<<ip);
		return false;
	}
	
	LOGI("connect success,addrs="<<ip<<",timeout="<<timeout);
	return true;
}

void LibRedisCluster::Fini()
{
	if(_redis != NULL)
	{
		redisClusterFree(_redis);
		_redis = NULL;
	}
}

bool LibRedisCluster::DoKeepAlive()
{	
	return true;
}

bool LibRedisCluster::Exists(const std::string& key, bool& result)
{
	redisReply* r = NULL;
	if(!doCommand(&r, "exists %s", key.c_str()))
	{
		return false;
	}
	
	bool ret = false;
	
	if(r->type != REDIS_REPLY_INTEGER)
	{
		LOGE("err r type,type="<<r->type);
		ret = false;
	}
	else
	{
		if(r->integer>0)
		{
			LOGD("key="<<key<<" is exists");
			ret = true;
		}
		else
		{
			LOGD("key="<<key<<" is not exists");
			ret = false;
		}
	}
	
	freeReplyObject(r);
	return ret;
}

bool LibRedisCluster::Del(const std::string& key)
{
	redisReply* r = NULL;
	if(!doCommand(&r, "del %s",key.c_str()))
	{
		return false;
	}
	
	bool ret = false;
	
	if(r->type != REDIS_REPLY_INTEGER)
	{
		LOGE("err r type,type="<<r->type);
		ret = false;
	}
	
	LOGD("inter="<<r->integer);
	freeReplyObject(r);
	return ret;
}

bool LibRedisCluster::Set_String(const std::string& key, const std::string& value, time_t ex)
{
	redisReply* r = NULL;
	if(!doCommand(&r,"set %s %s",key.c_str(),value.c_str()))
	{
		return false;
	}
	
	if(value.length()<=0)
	{
		LOGE("err value");
		return false;
	}
	
	bool ret = false;
	
	if(!(r->type == REDIS_REPLY_STATUS && strcasecmp(r->str,"OK")==0))
	{
		LOGE("err,key="<<key<<",value="<<value);
		ret = false;
	}
	else
	{
		LOGE("success,key="<<key<<",value="<<value);
		ret = setExpire(key,ex);
	}
	
	freeReplyObject(r);
	return ret;
}

bool LibRedisCluster::Get_String(const std::string& key, std::string& value)
{
	redisReply* r = NULL;
	if(!doCommand(&r,"get %s",key.c_str()))
	{
		return false;
	}
	
	bool ret = false;
	
	if(r->type != REDIS_REPLY_STRING)
	{
		LOGE("err,type="<<r->type);
		ret = false;
	}
	else
	{
		value = r->str;
		LOGD("type="<<r->type<<",str="<<value);
		ret = true;
	}
	
	freeReplyObject(r);
	return ret;
}

bool LibRedisCluster::setExpire(const std::string& key, time_t ex)
{
	redisReply* r = NULL;
	if(!doCommand(&r,"expire %s %d",key.c_str(),ex))
	{
		return false;
	}
	
	bool ret = false;
	
	if(!(r->type == REDIS_REPLY_INTEGER || r->type == REDIS_REPLY_STATUS && strcasecmp(r->str,"QUEUED")==0))
	{
		LOGE("err,key="<<key<<",ex="<<ex);
		ret = false;
	}
	else
	{
		LOGE("success,key="<<key);
		ret = true;
	}
	
	freeReplyObject(r);
	return ret;
}

/*************************** PRIVATE ***************************/
bool LibRedisCluster::doCommand(redisReply** reply,const char* format,...)
{
	if(!_redis)
	{
		LOGE("redis is null");
		return false;
	}
	
	va_list ap;
	va_start(ap,format);
	*reply = (redisReply*)redisClustervCommand(_redis, format, ap);
	va_end(ap);
	
	if(_redis->err)
	{
		LOGE("err="<<_redis->err<<",errstr="<<_redis->errstr);
		freeReplyObject(*reply);
		return false;
	}
	if(NULL == *reply)
	{
		LOGE("no reply");
		freeReplyObject(*reply);
		return false;
	}
	
	return true;
}










