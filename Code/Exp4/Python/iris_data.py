from sklearn import datasets
import numpy as np
iris = datasets.load_iris()
x, y = iris.data, iris.target
print(x[:,[1,3]])
# np.savetxt("/home/narip/Atom/Big_Data/iris_data",x[:,[1,3]])
