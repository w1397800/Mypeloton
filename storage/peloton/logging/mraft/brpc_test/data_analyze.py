def extract_values(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()

    # 初始化存储数据的列表
    total_flush_times = []
    average_flush_sizes = []
    total_flush_sizes = []
    average_latencies = []

    for line in lines:
        try:
            if "Total flush times for the last second:" in line:
                total_flush_times.append(int(line.split()[-1]))
            elif "Average flush size for the last second:" in line:
                # 将MB转换为浮点数
                average_flush_sizes.append(float(line.split()[-2]))
            elif "Total flush size for the last second:" in line:
                # 将MB转换为浮点数
                total_flush_sizes.append(float(line.split()[-2]))
            elif "Average latency for the last second:" in line:
                # 将ms转换为浮点数
                average_latencies.append(float(line.split()[-2]))
        except Exception as e:
            # 如果你想记录或打印错误，你可以在这里这样做
            # print(f"Error processing line: {line}. Error: {e}")
            continue


    return {
        "Average flush sizes": average_flush_sizes,
        "Average latencies": average_latencies,
        "Total flush sizes": total_flush_sizes,
        "Total flush times": total_flush_times,
    }

def calculate_metrics(values):
    metrics = {}
    for key, val_list in values.items():
        if val_list:
            metrics[key] = {
                "avg": sum(val_list)/len(val_list),
                "max": max(val_list),
                "min": min(val_list)
            }
    return metrics

values = extract_values('data.txt')
metrics = calculate_metrics(values)

for key, value in metrics.items():
    print(f"{key}:")
    print(f"Avg: {value['avg']:.4f}")
    print(f"Max: {value['max']}")
    print(f"Min: {value['min']}")
    print("----------")
