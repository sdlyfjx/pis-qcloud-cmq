const CMQ = require('../index')

const QCLOUD_SECRET_ID = 'QCLOUD_SECRET_ID'
const QCLOUD_SECRET_KEY = 'QCLOUD_SECRET_KEY'
const cmq_domain = 'https://cmq-queue-gz.api.qcloud.com'
const queue_name = 'queue_name'

const cmqApi = new CMQ(QCLOUD_SECRET_ID, QCLOUD_SECRET_KEY, cmq_domain)

describe('腾讯云消息队列', function() {
  it('SendMessageAsync', function(done) {
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
  })
})

