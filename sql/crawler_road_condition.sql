CREATE TABLE `crawler_road_condition` (
  `id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `province` varchar(50) NOT NULL,
  `publish_content` varchar(1000) NOT NULL,
  `publish_time` datetime(6) NOT NULL,
  `reason` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `section` varchar(255) DEFAULT NULL,
  `insert_time` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `primary_key` (`id`) USING BTREE,
  KEY `province_index` (`province`) USING BTREE,
  KEY `publish_index` (`publish_time` DESC) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;