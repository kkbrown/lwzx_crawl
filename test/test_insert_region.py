import csv
import hashlib
import pymysql
# 生成 MD5 哈希
def md5_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

# 加载 CSV 并写入数据库
def import_region_info(csv_path):
    # 连接 MySQL
    connection = pymysql.connect(
        host='192.168.1.200',
        user='cs',
        password='CG@#Vzc=Xri3lW+8Z=q',
        database='gz_cs',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]

    with connection:
        with connection.cursor() as cursor:
            for row in rows:
                province = row['province'].strip()
                city = row['city'].strip()
                area = row['area'].strip()
                region_id = md5_hash(province + city + area)

                sql = """
                INSERT IGNORE INTO region_info (id, province, city, area)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(sql, (region_id, province, city, area))

        connection.commit()
        print(f"已成功导入 {len(rows)} 条记录（重复会被忽略）")

# 示例入口
if __name__ == '__main__':
    import_region_info('D:\\pyWorkspace\\lwzx_crawl\\test\\region.csv')  # CSV 文件路径
