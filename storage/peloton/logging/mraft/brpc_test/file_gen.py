import os
import argparse

def create_file(filename, size_gb):
    # 将 GB 转换为 bytes
    size_bytes = size_gb * (1024 ** 3)

    # 使用 os.urandom 生成随机字节流
    with open(filename, 'wb') as f:
        f.write(os.urandom(size_bytes))
        print(f'文件 {filename} 已创建，大小为 {size_gb}GB')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='生成指定大小的文件')
    parser.add_argument('size_gb', type=int, help='文件大小（单位：GB）')

    args = parser.parse_args()

    create_file('testfile', args.size_gb)
