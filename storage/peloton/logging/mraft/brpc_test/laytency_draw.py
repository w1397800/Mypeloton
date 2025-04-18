import matplotlib.pyplot as plt

data = """
net[151->150]
 
 size: 1.000000KB
 latency: 52.8609us

 size: 2.000000KB
 latency: 53.2198us

 size: 4.000000KB
 latency: 60.281us

 size: 8.000000KB
 latency: 73.094us

 size: 16.000000KB
 latency: 88.9857us

 size: 32.000000KB
 latency: 129.665us

 size: 64.000000KB
 latency: 176.485us

 size: 128.000000KB
 latency: 252.668us

 size: 256.000000KB
 latency: 460.004us

 size: 512.000000KB
 latency: 693.904us

 size: 1.000000MB
 latency: 1132.46us

nvme：

size: 1.000000KB
latency: 8.28978us

size: 2.000000KB
latency: 16.4121us

size: 4.000000KB
latency: 33.0275us

size: 8.000000KB
latency: 64.6737us

size: 16.000000KB
latency: 86.5167us

size: 32.000000KB
latency: 73.409us

size: 64.000000KB
latency: 93.9175us

size: 128.000000KB
latency: 113.053us

size: 256.000000KB
latency: 180.881us

size: 512.000000KB
latency: 347.208us

size: 1.000000MB
latency: 743.489us

net[152->150]

size: 1.000000KB
latency: 19.9686us

size: 2.000000KB
latency: 38.9691us

size: 4.000000KB
latency: 78.8728us

size: 8.000000KB
latency: 103.007us

size: 16.000000KB
latency: 165.554us

size: 32.000000KB
latency: 196.081us

size: 64.000000KB
latency: 284.395us

size: 128.000000KB
latency: 432.197us

size: 256.000000KB
latency: 674.635us

size: 512.000000KB
latency: 1256.98us

size: 1.000000MB
latency: 2468.5us
"""

# 解析数据
sections = data.split('\n\n')
datasets = {}

for section in sections:
    lines = section.strip().split('\n')
    if not lines:
        continue
    label = lines[0]
    size = []
    latency = []
    for i in range(1, len(lines), 2):
        s = float(lines[i].split()[1].replace('KB', '').replace('MB', '000'))
        l = float(lines[i+1].split()[1].replace('us', ''))
        size.append(s)
        latency.append(l)
    datasets[label] = (size, latency)

# 绘制图形
plt.figure(figsize=(10, 6))

for label, (size, latency) in datasets.items():
    plt.plot(size, latency, label=label)

plt.xscale('log')
plt.yscale('log')
plt.xlabel('Size (KB)')
plt.ylabel('Latency (us)')
plt.title('Size vs Latency')
plt.legend()
plt.grid(True, which="both", ls="--", c='0.7')
plt.show()
