const Promise = require('bluebird')
const cmqClient = require('qcloudapi-sdk')
const baseHostInner = 'api.tencentyun.com'
const baseHostOutter = 'api.qcloud.com'

class CMQ_Queue {
  constructor(secretId, secretKey, region = 'bj', innerNet = false, debug = false) {
    this.cmq_client = new cmqClient({
      SecretId: secretId,
      SecretKey: secretKey,
      serviceType: 'cmq-queue-' + region,
      Region: region,
      RequestClient: 'kook-api-svr',
      baseHost: innerNet ? baseHostInner : baseHostOutter,
      protocol: innerNet ? 'http' : 'https'
    })

    // 主题相关接口
    this.CreateQueueAsync = Promise.promisify(this.CreateQueue)
    this.AssertQueueAsync = Promise.promisify(this.AssertQueue)
    this.ListQueueAsync = Promise.promisify(this.ListQueue)
    this.GetQueueAttributesAsync = Promise.promisify(this.GetQueueAttributes)
    this.SetQueueAttributesAsync = Promise.promisify(this.SetQueueAttributes)
    this.DeleteQueueAsync = Promise.promisify(this.DeleteQueue)
    // 消息相关接口
    this.SendMessageAsync = Promise.promisify(this.SendMessage)
    this.BatchSendMessageAsync = Promise.promisify(this.BatchSendMessage)
    this.ReceiveMessageAsync = Promise.promisify(this.ReceiveMessage)
    this.BatchReceiveMessageAsync = Promise.promisify(this.BatchReceiveMessage)
    this.DeleteMessageAsync = Promise.promisify(this.DeleteMessage)
    this.BatchDeleteMessageAsync = Promise.promisify(this.BatchDeleteMessage)
  }

  /**
   * 创建队列
   * @param {string} queueName 队列名称
   * @param {init} maxMsgHeapNum
   * @more https://www.qcloud.com/doc/api/431/5832
   */
  CreateQueue(queueName, maxMsgHeapNum = 10000000, pollingWaitSeconds = 0, visibilityTimeout = 30, maxMsgSize = 65536, msgRetentionSeconds = 345600, next = function () {}) {
    if (typeof maxMsgHeapNum === 'function') {
      next = maxMsgHeapNum
      maxMsgHeapNum = 10000000
    }

    this.cmq_client.request({
      Action: 'CreateQueue',
      queueName: queueName,
      maxMsgHeapNum: maxMsgHeapNum,
      pollingWaitSeconds: pollingWaitSeconds,
      visibilityTimeout: visibilityTimeout,
      maxMsgSize: maxMsgSize,
      msgRetentionSeconds: msgRetentionSeconds
    }, function (error, data) {
      next(error, data)
    })
  }

  /**
   * 取保获取课程队列，如果存在则返回true, 如果不存在则创建
   * @param  {[type]}   ) {}          [description]
   * @return {Function}   [description]
   */
  AssertQueue(queueName, maxMsgHeapNum = 10000000, pollingWaitSeconds = 0, visibilityTimeout = 30, maxMsgSize = 65536, msgRetentionSeconds = 345600, next = function () {}) {
    if (typeof maxMsgHeapNum === 'function') {
      next = maxMsgHeapNum
      maxMsgHeapNum = 10000000
    }

    this.cmq_client.request({
      Action: 'GetQueueAttributes',
      queueName: queueName
    }, function (error, data) {
      if (error) {
        return next(error, data)
      }

      if (data && data.code === 0) {
        return next(null, data)
      }

      if (data && data.code === 4440) {
        this.cmq_client.request({
          Action: 'CreateQueue',
          queueName: queueName,
          maxMsgHeapNum: maxMsgHeapNum,
          pollingWaitSeconds: pollingWaitSeconds,
          visibilityTimeout: visibilityTimeout,
          maxMsgSize: maxMsgSize,
          msgRetentionSeconds: msgRetentionSeconds
        }, function (error, data) {
          if (error) {
            return next(error)
          }

          // 4460为重复名称错误
          if (data && (
              data.code == 4460 ||
              (data.code == 4000 && data.message && data.message.indexOf('4460') >= 0))) {
            return next(null, data)
          }

          return next(data)
        })
      }

      return next(data)
    })
  }

  ListQueue(searchWord, offset = 0, limit = 20, next = function () {}) {
    if (typeof offset === 'function') {
      next = offset
      offset = 0
    }
    this.cmq_client.request({
      Action: 'ListQueue',
      searchWord: searchWord,
      offset: offset,
      limit: limit
    }, function (error, data) {
      next(error, data)
    })
  }

  GetQueueAttributes(queueName, next = function () {}) {
    this.cmq_client.request({
      Action: 'GetQueueAttributes',
      queueName: queueName
    }, function (error, data) {
      next(error, data)
    })
  }

  SetQueueAttributes(queueName, maxMsgHeapNum = 10000000, pollingWaitSeconds = 0, visibilityTimeout = 30, maxMsgSize = 65536, msgRetentionSeconds = 345600, next = function () {}) {
    if (typeof maxMsgHeapNum === 'function') {
      next = maxMsgHeapNum
      maxMsgHeapNum = 10000000
    }
    this.cmq_client.request({
      Action: 'SetQueueAttributes',
      queueName: queueName,
      maxMsgHeapNum: maxMsgHeapNum,
      pollingWaitSeconds: pollingWaitSeconds,
      visibilityTimeout: visibilityTimeout,
      maxMsgSize: maxMsgSize,
      msgRetentionSeconds: msgRetentionSeconds
    }, function (error, data) {
      next(error, data)
    })
  }

  DeleteQueue(queueName, next = function () {}) {
    this.cmq_client.request({
      Action: 'DeleteQueue',
      queueName: queueName
    }, function (error, data) {
      next(error, data)
    })
  }

  SendMessage(queueName, msgBody, next = function () {}) {
    this.cmq_client.request({
      Action: 'SendMessage',
      queueName: queueName,
      msgBody: msgBody
    }, function (error, data) {
      next(error, data)
    })
  }

  BatchSendMessage(queueName, msgBodys, next = function () {}) {
    let qBody = {
      Action: 'BatchSendMessage',
      queueName: queueName,
    }
    for (let i in msgBodys) {
      qBody[`msgBody.${i}`] = msgBodys[i]
    }
    this.cmq_client.request(qBody, function (error, data) {
      next(error, data)
    })
  }

  ReceiveMessage(queueName, pollingWaitSeconds = 0, next = function () {}) {
    this.cmq_client.request({
      Action: 'ReceiveMessage',
      queueName: queueName,
      pollingWaitSeconds: pollingWaitSeconds
    }, function (error, data) {
      next(error, data)
    })
  }

  BatchReceiveMessage(queueName, numOfMsg, pollingWaitSeconds, next = function () {}) {
    this.cmq_client.request({
      Action: 'BatchReceiveMessage',
      queueName: queueName,
      numOfMsg: numOfMsg,
      pollingWaitSeconds: pollingWaitSeconds
    }, function (error, data) {
      next(error, data)
    })
  }

  DeleteMessage(queueName, receiptHandle, next = function () {}) {
    this.cmq_client.request({
      Action: 'DeleteMessage',
      queueName: queueName,
      receiptHandle: receiptHandle
    }, function (error, data) {
      next(error, data)
    })
  }

  BatchDeleteMessage(queueName, receiptHandles, next = function () {}) {
    let qBody = {
      Action: 'BatchDeleteMessage',
      queueName: queueName
    }
    for (let i in receiptHandles) {
      qBody[`receiptHandle.${i}`] = receiptHandles[i]
    }
    this.cmq_client.request(qBody, function (error, data) {
      next(error, data)
    })
  }
}

class CMQ_Topic {
  constructor(secretId, secretKey, region = 'bj', innerNet = false, debug = false) {
    this.cmq_client = new cmqClient({
      SecretId: secretId,
      SecretKey: secretKey,
      serviceType: 'cmq-topic-' + region,
      Region: region,
      RequestClient: 'kook-api-svr',
      baseHost: innerNet ? baseHostInner : baseHostOutter,
      protocol: innerNet ? 'http' : 'https'
    })

    // 主题相关接口
    this.CreateTopicAsync = Promise.promisify(this.CreateTopic)
    this.AssertTopicAsync = Promise.promisify(this.AssertTopic)
    this.ListTopicAsync = Promise.promisify(this.ListTopic)
    this.GetTopicAttributesAsync = Promise.promisify(this.GetTopicAttributes)
    this.SetTopicAttributesAsync = Promise.promisify(this.SetTopicAttributes)
    this.DeleteTopicAsync = Promise.promisify(this.DeleteTopic)
    // 消息相关接口
    this.PublishMessageAsync = Promise.promisify(this.PublishMessage)
    this.BatchPublishMessageAsync = Promise.promisify(this.BatchPublishMessage)
    // 订阅相关接口
    this.SubscribeAsync = Promise.promisify(this.Subscribe)
    this.UnsubscribeAsync = Promise.promisify(this.Unsubscribe)
    this.ListSubscriptionByTopicAsync = Promise.promisify(this.ListSubscriptionByTopic)
    this.GetSubscriptionAttributesAsync = Promise.promisify(this.GetSubscriptionAttributes)
    this.SetSubscriptionAttributesAsync = Promise.promisify(this.SetSubscriptionAttributes)
    this.ClearSubscriptionFilterTagsAsync = Promise.promisify(this.ClearSubscriptionFilterTags)
  }

  /**
   * 创建主题
   * @param {string} topicName 主题名称
   * @param {int} maxMsgSize 消息最大长度。取值范围1024 - 65536 Byte（即1 - 64K），默认值65536。
   * @param {int} filterType 用于指定主题的消息匹配策略：filterType =1 或为空， 表示该主题下所有订阅使用 filterTag 标签过滤；filterType =2 表示用户使用 bindingKey 过滤。注意：该参数设定之后不可更改。
   * @more https://cloud.tencent.com/document/api/406/7405
   */
  CreateTopic(topicName, maxMsgSize = 65536, filterType = 0, next = function () {}) {
    if (typeof maxMsgSize === 'function') {
      next = maxMsgSize
      maxMsgSize = 65536
    }

    this.cmq_client.request({
      Action: 'CreateTopic',
      topicName: topicName,
      maxMsgSize: maxMsgSize,
      filterType: filterType
    }, function (error, data) {
      next(error, data)
    })
  }

  /**
   * 确保获取主题列表，如果存在则返回true, 如果不存在则创建
   * @param  {[type]}   ) {}          [description]
   * @return {Function}   [description]
   */
  AssertTopic(topicName, maxMsgSize = 65536, filterType = 0, next = function () {}) {
    if (typeof maxMsgSize === 'function') {
      next = maxMsgSize
      maxMsgSize = 65536
    }

    this.cmq_client.request({
      Action: 'GetTopicAttributes',
      topicName: topicName
    }, function (error, data) {
      if (error) {
        return next(error, data)
      }

      if (data && data.code === 0) {
        return next(null, data)
      }

      if (data && data.code === 4440) {
        this.cmq_client.request({
          Action: 'CreateTopic',
          topicName: topicName,
          maxMsgSize: maxMsgSize,
          filterType: filterType
        }, function (error, data) {
          if (error) {
            return next(error)
          }

          // 4460为重复名称错误
          if (data && (
              data.code == 4460 ||
              (data.code == 4000 && data.message && data.message.indexOf('4460') >= 0))) {
            return next(null, data)
          }

          return next(data)
        })
      }

      return next(data)
    })
  }


  ListTopic(searchWord, offset = 0, limit = 20, next = function () {}) {
    if (typeof offset === 'function') {
      next = offset
      offset = 0
    }
    this.cmq_client.request({
      Action: 'ListTopic',
      searchWord: searchWord,
      offset: offset,
      limit: limit
    }, function (error, data) {
      next(error, data)
    })
  }

  GetTopicAttributes(topicName, next = function () {}) {
    this.cmq_client.request({
      Action: 'GetTopicAttributes',
      topicName: topicName
    }, function (error, data) {
      next(error, data)
    })
  }

  SetTopicAttributes(topicName, maxMsgSize = 65536, next = function () {}) {
    if (typeof maxMsgSize === 'function') {
      next = maxMsgSize
      maxMsgSize = 65536
    }
    this.cmq_client.request({
      Action: 'SetTopicAttributes',
      topicName: topicName,
      maxMsgSize: maxMsgSize,
    }, function (error, data) {
      next(error, data)
    })
  }

  DeleteTopic(topicName, next = function () {}) {
    this.cmq_client.request({
      Action: 'DeleteTopic',
      topicName: topicName
    }, function (error, data) {
      next(error, data)
    })
  }

  /**
   * 发布消息到主题队列
   * https://cloud.tencent.com/document/api/406/7411
   * @param {string} topicName 主题名字，在单个地域同一帐号下唯一。主题名称是一个不超过64个字符的字符串，必须以字母为首字符，剩余部分可以包含字母、数字和横划线(-)。
   * @param {string} msgBody 消息正文。至少1Byte，最大长度受限于设置的主题消息最大长度属性。
   * @param {[string]} msgTag 消息过滤标签。
   * @param {string} routingKey 
   * @param {Function} next 
   */
  PublishMessage(topicName, msgBody, msgTag, routingKey, next = function () {}) {
    if (typeof msgTag === 'function') {
      next = msgTag
      msgTag = []
      routingKey = ''
    }

    let qBody = {
      Action: 'PublishMessage',
      topicName: topicName,
      msgBody: msgBody,
    }

    if (routingKey.length > 0)
      qBody[routingKey] = routingKey

    // 标签数量不能超过5个，每个标签不超过16个字符
    for (let i = 0, l = msgTag.length; i < l && i < 5; i++) {
      qBody[`msgTag.${i}`] = msgTag[i]
    }

    this.cmq_client.request(qBody, function (error, data) {
      next(error, data)
    })
  }

  BatchPublishMessage(topicName, msgBodys, msgTag, routingKey, next = function () {}) {
    if (typeof msgTag === 'function') {
      next = msgTag
      msgTag = []
      routingKey = ''
    }

    let qBody = {
      Action: 'BatchPublishMessage',
      topicName: topicName
    }

    if (routingKey.length > 0)
      qBody[routingKey] = routingKey

    for (let i in msgBodys) {
      qBody[`msgBody.${i}`] = msgBodys[i]
    }
    for (let j in msgTag) {
      qBody[`msgTag.${j}`] = msgTag[j]
    }
    this.cmq_client.request(qBody, function (error, data) {
      next(error, data)
    })
  }

  /**
   * 本接口 (Subscribe) 用于在用户某个主题下创建一个新订阅。
   * https://cloud.tencent.com/document/api/406/7414
   * @param {*} topicName 
   * @param {*} subscriptionName 
   * @param {*} protocol 
   * @param {*} endpoint 
   * @param {*} notifyStrategy 
   * @param {*} notifyContentFormat 
   * @param {*} filterTags 
   * @param {*} bindingKeys 
   * @param {*} next 
   */
  Subscribe(topicName, subscriptionName, protocol, endpoint, notifyStrategy = 'EXPONENTIAL_DECAY_RETRY', notifyContentFormat = 'JSON', filterTags, bindingKeys, next = function () {}) {
    if (typeof notifyStrategy === 'function') {
      next = notifyStrategy
      notifyStrategy = 'EXPONENTIAL_DECAY_RETRY'
    }

    let qBody = {
      Action: 'Subscribe',
      topicName: topicName,
      subscriptionName: subscriptionName,
      protocol: protocol,
      endpoint: endpoint,
      notifyStrategy: notifyStrategy,
      notifyContentFormat: notifyContentFormat
    }

    for (let i in filterTags) {
      qBody[`filterTag.${i}`] = filterTags[i]
    }
    for (let j in bindingKeys) {
      qBody[`bindingKey.${j}`] = bindingKeys[j]
    }

    this.cmq_client.request(qBody, function (error, data) {
      next(error, data)
    })
  }

  Unsubscribe(topicName, subscriptionName, next = function () {}) {
    this.cmq_client.request({
      Action: 'Unsubscribe',
      topicName: topicName,
      subscriptionName: subscriptionName
    }, function (error, data) {
      next(error, data)
    })
  }

  ListSubscriptionByTopic(topicName, searchWord, offset = 0, limit = 20, next = function () {}) {
    if (typeof searchWord === 'function') {
      next = searchWord
      searchWord = ''
    }
    this.cmq_client.request({
      Action: 'ListSubscriptionByTopic',
      topicName: topicName,
      searchWord: searchWord,
      offset: offset,
      limit: limit
    }, function (error, data) {
      next(error, data)
    })
  }

  GetSubscriptionAttributes(topicName, subscriptionName, next = function () {}) {
    this.cmq_client.request({
      Action: 'GetSubscriptionAttributes',
      topicName: topicName,
      subscriptionName: subscriptionName
    }, function (error, data) {
      next(error, data)
    })
  }

  SetSubscriptionAttributes(topicName, subscriptionName, notifyStrategy = 'EXPONENTIAL_DECAY_RETRY', notifyContentFormat = 'JSON', filterTags, bindingKeys, next = function () {}) {
    if (typeof notifyStrategy === 'function') {
      next = notifyStrategy
      notifyStrategy = 'EXPONENTIAL_DECAY_RETRY'
    }

    let qBody = {
      Action: 'SetSubscriptionAttributes',
      topicName: topicName,
      subscriptionName: subscriptionName,
      protocol: protocol,
      endpoint: endpoint,
      notifyStrategy: notifyStrategy,
      notifyContentFormat: notifyContentFormat
    }

    for (let i in filterTags) {
      qBody[`filterTag.${i}`] = filterTags[i]
    }
    for (let j in bindingKeys) {
      qBody[`bindingKey.${j}`] = bindingKeys[j]
    }

    this.cmq_client.request(qBody, function (error, data) {
      next(error, data)
    })
  }

  ClearSubscriptionFilterTags(topicName, subscriptionName, next = function () {}) {
    this.cmq_client.request({
      Action: 'ClearSubscriptionFilterTags',
      topicName: topicName,
      subscriptionName: subscriptionName
    }, function (error, data) {
      next(error, data)
    })
  }

}

module.exports = {
  CMQQ: CMQ_Queue,
  CMQT: CMQ_Topic
}