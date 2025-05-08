CREATE TABLE `province_road_condition` (
  `id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '主键ID',
  `province` varchar(50) NOT NULL COMMENT '省份名称',
  `road_code` varchar(50) DEFAULT NULL COMMENT '高速公路编号',
  `road_name` varchar(50) DEFAULT NULL COMMENT '高速公路名称',
  `publish_content` varchar(512) DEFAULT NULL COMMENT '发布内容',
  `publish_time` datetime(6) NOT NULL COMMENT '发布时间',
  `start_time` datetime(6) DEFAULT NULL COMMENT '开始时间',
  `end_time` datetime(6) DEFAULT NULL COMMENT '结束时间',
  `insert_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据插入时间',
  `event_type_name` enum('施工养护','交通事故','交通管制','异常天气','其他事件') DEFAULT NULL COMMENT '事件类型（施工养护、交通事故、交通管制、异常天气、其他事件）',
  `event_category` enum('实时事件','计划事件') DEFAULT NULL COMMENT '事件类别（实时事件或计划事件）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `primary_key` (`id`) USING BTREE,
  KEY `province_index` (`province`) USING BTREE,
  KEY `publish_index` (`publish_time` DESC) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;