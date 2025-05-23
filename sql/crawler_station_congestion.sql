CREATE TABLE `crawler_station_congestion` (
  `id` varchar(255) NOT NULL COMMENT '主键ID（如UUID）',
  `publish_time` datetime NOT NULL COMMENT '发布时间',
  `province_name` varchar(50) NOT NULL COMMENT '省份名称',
  `city_name` varchar(50) NOT NULL COMMENT '城市名称',
  `road_name` varchar(50) NOT NULL COMMENT '路段名称',
  `station_name` varchar(50) NOT NULL COMMENT '收费站名称',
  `station_rank` int NOT NULL COMMENT '排名',
  `congest_length` decimal(6,2) NOT NULL COMMENT '拥堵里程（单位：公里）',
  `avg_speed` decimal(5,2) NOT NULL COMMENT '平均速度（单位：公里/小时）',
  `batch_num` varchar(255) DEFAULT NULL COMMENT '批次号',
  PRIMARY KEY (`id`),
  KEY `publish_index` (`publish_time` DESC),
  KEY `province_index` (`province_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='收费站交通拥堵统计表';