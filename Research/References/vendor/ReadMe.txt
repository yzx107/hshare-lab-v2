data 目录，是捷利交易宝提供的每个交易日港股全盘特色数据。

一、目录介绍
1、data 目录下以 "YYYYMMDD.zip" zip压缩存放每个交易日的数据；

2、将 zip 文件解压后，里面有三个子目录;
    (1) OrderAdd: 增加委托
    (2) OrderModifyDelete: 修改和删除委托
    (3) TradeResumes: 逐笔还原
   备注: 从2026年1月1日开始只有只有两个目录(分别是：order和trade)

3、每个子目录下以 "港股证券代码.csv" 存放每个股票证券的数据；

二、数据介绍
1、OrderAdd 和 OrderModifyDelete 数据项(备注: 从2026年1月1日开始都合并到order)：
    (1) SeqNum: 序列号
    (2) OrderId: 委托ID号
    (3) OrderType: 委托类型. 1: 增加 2: 修改 3: 删除
    (4) Ext: 扩展. 第1位: 委托买卖类型 0:买 1:卖
                   第2位: 档位买卖类型 0:买 1:卖
                   第3位: 当委托类型是: 增加，则 0：限价单 1：市价单
                          当委托类型是: 修改，则 0：改单 1：成交
                          当委托类型是: 删除，则 0：撤单 1：成交

    (5) Time: 时间. 格式:HHMMSS
    (6) Price: 委托价
    (7) Volume: 委托量
    (8) Level: 买卖盘档位. 0表示第一档，1表示第二档，以此类推
    (9) BrokerNo: 经纪商席位号
    (10) VolumePre: 修改前的量, 仅委托类型是修改时有效

2、TradeResumes 数据项(备注: 从2026年1月1日开始改名为trade)：
    (1) Time: 时间. 格式:HHMMSS
    (2) Price: 成交价
    (3) Volume: 成交量
    (4) Dir: 方向. 1: 绿色向下箭头，代表卖方主动性成交已下单的买单；2: 红色向上箭头，代表买方主动性成交已下单的卖单；0: 其它
    (5) Type: 类型. 
    (6) BrokerNo: 经纪商席位号
    (7) TickID: 成交明细ID号
    (8) BidOrderID: 买盘委托ID
    (9) BidVolume: 买盘委托量
    (10) AskOrderID: 卖盘委托ID
    (11) AskVolume: 卖盘委托量
