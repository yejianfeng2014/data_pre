# data_pre
数据预处理过程

 1，从原始数据库读取邮件主题和内容（邮件内容是html的格式）
 
 2，解析html 格式的内容，拿到邮件正文 ，
 
 3，邮件的语言进行检测，
 
 4，修改邮件语言的检测错误的结果
 
 5，预处理好的数据导出成txt 格式
 
  格式如下 邮件主题 # 邮件内容
  
  示例如下： Others # not received order
  
  spark 在window 写入文件存在问题，这个数据建议从数据库直接导出   
  
  
  算法模型如下： 这个部分在python 代码里面实现
  
    根据上面的预处理好的模型创建一个分类的模型，实现根据邮件内容自动分类出属于哪个主题
    
    分类模型构建如下：
                   1，邮件内容分词
                   2，标点符号处理
                   3，词编码
                   4，输入编码后的数据
                   
                   5，把数据喂给训练好的模型 ###提前构建模型，如果采用java需要自己手动进行解码和编码 
                   
                   6，模型输出结果解码
                   7，返回输出结果
                    
                    
                    TODO 研究这个模型
                  https://github.com/brightmart/text_classification.git   
                  
                  
                  
                  https://github.com/brightmart/bert_language_understanding
                  