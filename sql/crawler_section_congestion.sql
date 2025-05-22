CREATE TABLE `crawler_section_congestion` (
  `id` varchar(255) NOT NULL COMMENT '主键ID（如UUID）',
  `publish_time` datetime NOT NULL COMMENT '发布时间',
  `province_name` varchar(50) NOT NULL COMMENT '省份名称',
  `road_name` varchar(50) NOT NULL COMMENT '路段名称',
  `section_rank` int NOT NULL COMMENT '排名',
  `congest_length` decimal(6,2) NOT NULL COMMENT '拥堵里程（单位：公里）',
  `avg_speed` decimal(5,2) NOT NULL COMMENT '平均速度（单位：公里/小时）',
  `batch_num` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '批次号',
  `semantic` varchar(255) DEFAULT NULL COMMENT '方向',
  PRIMARY KEY (`id`),
  KEY `publish_index` (`publish_time` DESC) USING BTREE,
  KEY `province_index` (`province_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='路段交通拥堵统计表';