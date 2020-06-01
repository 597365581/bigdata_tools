# yongqing-log-output

大数据日志采集--日志统一输出
<img alt="jiagou1.png" class="js-lazy-loaded" src="http://gitlab.yongqing.com/bigdata/yongqing-bigdata-tools/raw/master/yongqing-bigdata-tools/yongqing-log-output/docs/jiagou1.png">

****1、	目的*****

指定统一的业务日志输出规范，便于未来业务日志通过统一采集的方式进入大数据平台，做业务分析和统计。

****2、	日志采集类型****
<p>系统请求日志:请求调用分析，一般用于对外提供的标准的API接口服务的系统请求日志的采集，一般适合入hive和Hbase</p>
<p>系统日志:用于ELK分析，用于分析系统异常日志情况</p>
<p>业务日志:业务数据分析，业务数据的采集</p>
<p>Nginx日志:接口调用分析，用于分析每个http接口或者页面的调用量，调用的成功率等，这种直接从nginx端采集，可以入Hive和ES</p>

**2.1、	系统请求日志**

固定规范：日志输出时，以json的形式输出.

**通用字段**

<p>请求发起时间	requestInitiationTime	非必填，一般是用户传入，格式：yyyy-MM-dd HH:mm:ss.SSS</p>
<p>请求接收时间	requestReceiveTime	必填，系统实际接收到请求的时间，格式：yyyy-MM-dd HH:mm:ss.SSS</p>
<p>请求日志生成时间	requestLogTime	必填，统一获取(日志打印的时间)，格式：yyyy-MM-dd HH:mm:ss.SSS</p>
<p>请求的IP地址	requestInitiationIp	必填，请求发起的IP地址，统一获取</p></p>
<p>请求处理的IP	requestHostIp	必填，请求实际被处理的系统的机器IP，统一获取</p></p>
<p>请求的服务	requestApi	必填，请求的系统服务，统一获取</p>
<p>请求的url	requestUrl	必填，请求的Url地址，统一获取</p>
<p>请求的参数	requestParameter	必填，请求的参数，统一获取</p>
<p>请求处理完成时间	requestCompletionTime	必填，请求处理完成的时间，统一获取，格式：yyyy-MM-dd HH:mm:ss.SSS</p>
<p>请求的处理系统	requestHostSystem	请求实际被处理的系统(系统的唯一英文简称)</p>
<p>请求响应的Code	requestCompletionCode	必填</p>
<p>请求结果	requestCompletionResult	必填</p>
<p>请求id	requestId	必填，系统统一生成</p>
<p>请求的应用ID	appId	非必填，主要针对应用接入的情况，比如需要先注册接入，接入后，会分配一个唯一的应用ID，请求时需要传入</p>
<p>请求的签名	sign	非必填，和appId结合使用，用于接口请求的校验</p>
<p>自定义字段	customizeField	这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ “key1”:”value1”,”key2”:”value2” }</p>
<p>采集标志	collectionSign	必填，采集标志，用于采集判断，不带此标志的数据不采集。固定为systemRequestCollection</p>

**2.2、	业务日志**

固定规范：日志输出时，以json的形式输出.

**通用字段**

<p>系统名称	businessSystem	必填，业务系统(系统的唯一英文简称)</p>
<p>业务id	businessId	必填，业务ID（每个业务不可重复，建议未来统一生成和分配，在接入统一采集）</p>
<p>业务类型	businessType	必填，业务类型（统一定义）</p>
<p>业务名称	businessName	必填，业务名称</p>
<p>业务日志打印时间	businessLogTime	必填，系统统一生成获取(业务日志打印时间)，格式：yyyy-MM-dd HH:mm:ss.SSS</p>
<p>日志ID	businessLogId	必填，系统统一生成获取</p>
<p>自定义字段	customizeField	这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ "key1":"value1","key2":"value2" }</p>
<p>采集标志	collectionSign	必填，采集标志，用于采集判断，不带此标志的数据不采集。固定为 businessCollection</p>

**2.3、	Nginx日志**

Nginx日志，所有系统使用统一的nginx.conf日志配置，然后输出的nginx 请求日志字段完全一致。

**2.4、	系统日志**

系统日志只对日志文件名称统一规范，然后生成的日志统一采集入到ELK中，包括Info日志，error和异常等日志。

名称规范:系统简称.log，在此格式上按照yyyy-MM-dd统一自动归档。
