# Aix.MessageBus
程序启动参考Sample
发布和订阅消息支持分布式部署

1 配置kafka信息,参考Sample
2 启动服务，参考Sample

3 发布消息
 var messageData = new KafkaMessage { MessageId = i.ToString(), Content = $"我是内容_{i}", CreateTime = DateTime.Now };
  await _messageBus.PublishAsync(messageData);
  
  4 订阅消息 根据泛型类型区分不同类型的订阅
   await _messageBus.SubscribeAsync<KafkaMessage>(async (message) =>
   {
       var current = Interlocked.Increment(ref Count);
       Console.WriteLine($"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff")}消费数据：MessageId={message.MessageId},Content=                         {message.Content},count={current}");
       await Task.CompletedTask;
   });
 
