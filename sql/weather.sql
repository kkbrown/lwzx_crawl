CREATE TABLE `weather` (
  `id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '主键ID',
  `province` varchar(50) NOT NULL COMMENT '省份名称',
  `city` varchar(50) DEFAULT NULL COMMENT '市名称',
  `area` varchar(50) DEFAULT NULL COMMENT '区名称',
  `title` varchar(255) DEFAULT NULL COMMENT '标题',
  `warning_level` varchar(50) NOT NULL COMMENT '预警等级',
  `warning_type` varchar(50) NOT NULL COMMENT '预警类型',
  `warning_content` TEXT NOT NULL COMMENT '预警内容',
  `publish_time` datetime(6) NOT NULL COMMENT '发布时间',
  `insert_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据插入时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `primary_key` (`id`) USING BTREE,
  KEY `province_index` (`province`) USING BTREE,
  KEY `warning_type_index` (`warning_type` DESC) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
