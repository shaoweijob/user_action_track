## 案例：各区域热门商品统计（主要技术SparkSQL）

### 项目开发整体流程
1. 需求分析
2. 方案设计（技术选型）
3. 数据设计
4. 编码实现（功能）
5. 测试（小数据量 -> 大数据量 -> 测试机）
6. 生产（试运行）

### 商品行为
* 点击行为
* 下单行为
* 支付行为

### 各区域热门商品统计
* 需求： 商品、热门（点击行为）Top10，区域
* 分析的数据
	* 用户访问行为数据 user_visit_action
	* 进行筛选过滤 date
	* 区域：city_id
	* 点击商品 click_product_id
	* 依据不同维度进行分组、统计、排序、TopKey

### 维度信息表
* 城市信息表 city_info   ->  MySQL关系型数据库
	* city_id
	* city_name
	* area
	* ......

### 商品信息表 product_info -> Hive 表中
* product_id
* product_name
* extend_info
	* JSON 格式数据
	* product_status（0,自营商品;1,第三方商品)

### 使用Hive/SparkSQL/SQL on HADOOP
* 企业开发，统计



* -b,分组

	可能会涉及到与基础信息表进行JOIN

	按照不同的类别（依据某个字段）进行分类（分组）
* -c,统计

	Count，是否去重

	类似WordCount
* -d,排序

	通常情况下，依据统计字段统计值，降序，涉及到一些维度

* -e,TopKey

	取前多少位

	ROW_NUMBER

### SparkSQL性能优化，技巧点
	sqlContext.read.jdbc() // 多个重载方法，暗示不同场景选择不同方式，达到性能

	dataFrame.write.
