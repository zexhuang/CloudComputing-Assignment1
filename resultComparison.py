import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
 
objects = ('1 node and 1 core', '1 node and 8 cores', '2 nodes and 8 cores')
y_pos = np.arange(len(objects))
performance = [64.965988, 47.490299,47.507174]
 
plt.bar(y_pos, performance, align='center', alpha=0.5)
plt.xticks(y_pos, objects)
plt.ylabel('Time (Sceonds)')
plt.title('Performance')
 
plt.show()