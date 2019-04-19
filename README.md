# fjx-qcloud-cmq

腾讯云CMQ相关的NodeSDK，人生的第一次NPM发布，在前辈基础上做的修改。

=======

[![NPM](https://nodei.co/npm-dl/fjx-qcloud-cmq.png)](https://nodei.co/npm/fjx-qcloud-cmq/)
[![NPM](https://nodei.co/npm/fjx-qcloud-cmq.png)](https://nodei.co/npm/fjx-qcloud-cmq/)

感谢原作者PISTech的项目[`pis-qcloud-cmq`](https://github.com/pistech/pis-qcloud-cmq)，在此基础上做了如下修改：

1. 修改了初始化对象方式，由url改为region模式
1. 适配腾讯云内网域名（内网环境下调用不计流量费）
1. 新增Topic模型下的API
1. 包括队列模型和主题模型的所有API，[官方文档](https://cloud.tencent.com/document/api/406/5852)

<a name="install"></a>
## Installation

```sh
npm install fjx-qcloud-cmq --save
```

<a name="example"></a>
## Example

```js
// 队列模型
const MQ = require('fjx-qcloud-cmq').CMQQ
// 主题模型
const MT = reuqire('fjx-qcloud-cmq').CMQT
// 填写相关配置，设置是否为内网模式(默认为外网调用)
const cmqApi = new MQ('SecretId','SecretKey','Region',false)
// 根据文档调用方法即可
cmqApi.AssertQueueAsync(queue_name).then(function(q){
      return q
    }).then(() => {
      // 发送书籍推送到消息队列
      return cmqApi.SendMessageAsync(queue_name, 'test').then(function(res){
        console.log(res)
        
        // 发送失败(更新状态)
        if (res.code) {
          return done(res)
        }

        return done()
      })
    }).catch((err) => {
      console.log(err)

      done(err)
    })
```