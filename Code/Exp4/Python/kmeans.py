import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets
color = ['#F0F8FF','#FAEBD7','#00FFFF','#7FFFD4','#F0FFFF','#F5F5DC','#FFE4C4','#000000','#FFEBCD','#0000FF','#8A2BE2','#A52A2A','#DEB887','#5F9EA0','#7FFF00','#D2691E','#FF7F50','#6495ED','#FFF8DC','#DC143C','#00FFFF','#00008B','#008B8B','#B8860B','#A9A9A9','#006400','#BDB76B','#8B008B','#556B2F','#FF8C00','#9932CC','#8B0000','#E9967A','#8FBC8F','#483D8B','#2F4F4F','#00CED1','#9400D3','#FF1493','#00BFFF','#696969','#1E90FF','#B22222','#FFFAF0','#228B22','#FF00FF','#DCDCDC','#F8F8FF','#FFD700','#DAA520','#808080','#008000','#ADFF2F','#F0FFF0','#FF69B4','#CD5C5C','#4B0082','#FFFFF0','#F0E68C','#E6E6FA','#FFF0F5','#7CFC00','#FFFACD','#ADD8E6','#F08080','#E0FFFF','#FAFAD2','#90EE90','#D3D3D3','#FFB6C1','#FFA07A','#20B2AA','#87CEFA','#778899','#B0C4DE','#FFFFE0','#00FF00','#32CD32','#FAF0E6','#FF00FF','#800000','#66CDAA','#0000CD','#BA55D3','#9370DB','#3CB371','#7B68EE','#00FA9A','#48D1CC','#C71585','#191970','#F5FFFA','#FFE4E1','#FFE4B5','#FFDEAD','#000080','#FDF5E6','#808000','#6B8E23','#FFA500','#FF4500','#DA70D6','#EEE8AA','#98FB98','#AFEEEE','#DB7093','#FFEFD5','#FFDAB9','#CD853F','#FFC0CB','#DDA0DD','#B0E0E6','#800080','#FF0000','#BC8F8F','#4169E1','#8B4513','#FA8072','#FAA460','#2E8B57','#FFF5EE','#A0522D','#C0C0C0','#87CEEB','#6A5ACD','#708090','#FFFAFA','#00FF7F','#4682B4','#D2B48C','#008080','#D8BFD8','#FF6347','#40E0D0','#EE82EE','#F5DEB3','#FFFFFF','#F5F5F5','#FFFF00','#9ACD32']
def _distance(x1,y1,x2,y2):
    return np.sqrt((x1-x2)**2+(y1-y2)**2)

def _rand_center(data,k):
    min_x = np.min(x)
    max_x = np.max(x)
    min_y = np.min(y)
    max_y = np.max(y)
    center = np.zeros((k,2))
    center[:,0] = min_x + (max_x - min_x)*np.random.rand(k)
    center[:,1] = min_y + (max_y - min_y)*np.random.rand(k)
    return center

def kmeans():
    for i in range(x.shape[0]):
        dist = np.inf
        for j in range(k):
            if(_distance(x[i],y[i],center[j][0],center[j][1]) < dist):
                dist = _distance(x[i],y[i],center[j][0],center[j][1])
                tag = j
        label[i] = tag
    for i in range(k):
        index = np.where(label == i)
        center[i][0] = np.sum(x[index])/len(index[0])
        center[i][1] = np.sum(y[index])/len(index[0])

iris = datasets.load_iris()
data = iris.data
x = data[:,[1]]
y = data[:,[3]]
# label stands for what cluster points belong to
label = np.zeros(x.size,dtype=int)
k = 2
center = _rand_center(data,k)
old_center = np.ones((k,2))
while(not (old_center == center).all()):
    old_center =np.copy(center)
    kmeans()
fig, (ax1,ax2) = plt.subplots(1,2,figsize=(12,5))
ax1.scatter(x,y,c='b')
for i in range(k):
    index = np.where(label == i)
    ax2.scatter(x[index],y[index],c=color[i+10])
ax2.scatter(center[:,0],center[:,1],c='black',s=120,marker='o')
# [2.872 3.428]
# [1.676 0.246]
print(center)
plt.show()
