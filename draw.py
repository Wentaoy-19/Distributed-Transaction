import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import sys
from matplotlib.ticker import MaxNLocator
import os

if __name__ == '__main__':
    # node_num=sys.argv[1]
    # fail_num=sys.argv[2]
    # case_id=sys.argv[3]
    case_id=sys.argv[1]
    msg_id_dict={}
    bandwidth_list=[]
   
    path = "logs" 
    files= os.listdir(path)
    
    for file in files: 
        content_list=[]
        if not os.path.isdir(file): 
            f = open(path+"/"+file); 
            iter_f = iter(f); 
            str = ""
            for line in iter_f: 
                # print(line)
                # str = str + line
                if line.find('[IGNORE]')!=-1 or line.find('[SUCCESS]')!=-1:
                    # print('here')
                    content_list.append(line.split(':'))
            # s.append(str) 
                else:
                    connect_time=float(line)
                    # print(connect_time)
            msg_count=len(content_list)
            
            bandwidth_list.append(round(msg_count/connect_time,3))
            # print(bandwidth_list)
            # print(msg_count/connect_time)
        # print(content_list)
            
        # content_list=content_list[0:-1]
        for msg_list in content_list:
            if msg_list[1] in msg_id_dict:
                msg_id_dict[msg_list[1]].append(float(msg_list[2]))
            else:
                msg_id_dict[msg_list[1]]=[float(msg_list[2])]
    x=[(i+1) for i in range(0,len(bandwidth_list))]
    # print(bandwidth_list)
    plt.figure(figsize=(8, 6))
    plt.title(f'Case {case_id}, Bandwidth')
    plt.xlabel('Node id')
    # plt.xlim(1, len(x))
    # plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
    plt.ylabel('Bandwidth')
    plt.plot(x,bandwidth_list)
    plt.savefig(f'./pic/case_{case_id}_bandwidth.jpg')
    second_metric_list=[]
    for k in sorted(msg_id_dict.keys()):
        second_metric=max(msg_id_dict[k])-min(msg_id_dict[k])
        second_metric_list.append(second_metric)
    x=[i for i in range(len(second_metric_list))]
    plt.figure(figsize=(8, 6))
    plt.title(f'Case {case_id}, Second metric')
    plt.xlabel('Message in send order')
    plt.ylabel('time')
    plt.plot(x,second_metric_list)
    plt.savefig(f'./pic/case_{case_id}_second_metric.jpg')


