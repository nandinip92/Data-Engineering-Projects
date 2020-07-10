# https://atom.io/packages/script
import os
import json
from typing import List, Dict


class Label:
    def __init__(self, name: str):
        self.name = name
        self.values = list()

    def add_value(self, value):
        self.values.append(value)


class LabelDescriptorToJson:
    """ Function to read the LabelDecriptor.SAS file and convert into json
    """

    def __init__(self, inpt_file: str, labels: List[str], opt_file: str):
        self.inpt_file = inpt_file
        self.labels = labels
        self.opt_file = opt_file
        self.objs = list()
        self._ProcessFile_()
        self._WriteToJsonFile()

    def _ProcessFile_(self):
        """
        Processing the SAS file and converting into JSON
        """
        fh = open(self.inpt_file)
        my_label = None
        parse_values = False
        
        for line in fh:
            if "=" in line and my_label:
                tokens = line.split("=")
                key = tokens[0].strip().strip("'").strip(";").strip().strip("'").strip()
                value = (
                    tokens[1].strip().strip("'").strip(";").strip().strip("'").strip()
                )
                my_label.add_value({"key": key, "val": value})
            else:       
                if any(a for a in self.labels if a in line):
                    if my_label:
                        self.objs.append(my_label)
                    label_name = None
                    for w in line.split():
                        if w in self.labels:
                            if w == "I94CIT" or w == "I94RES":
                                w = "I94CIT_I94RES"
                            label_name = w
                            break
                    my_label = Label(label_name)

        self.objs.append(my_label)
        # j = [x.__dict__ for x in self.objs]
        # out_file = open('test.json','w')
        # out_file.write(json.dumps(j))#,indent=4
        fh.close()

    def _WriteToJsonFile(self):
        json_data = [{x.name: x.values} for x in self.objs]
        out_file = open(self.opt_file, "w+")
        print(" Creating SAS Label Descriptor JSON file at : {}".format(self.opt_file))
        out_file.write(json.dumps(json_data))


"""
if __name__ == '__main__':
    #path = os.getcwd()
    #print("Current Working Directory: ",path)
    #file = os.path.join(path,'I94_SAS_Labels_Descriptions.SAS')
    inpt_file = 'Datasets/I94_SAS_Labels_Descriptions.SAS'
    labels = ['I94CIT', 'I94RES','I94PORT','I94MODE','I94ADDR','I94VISA']
    opt_file = 'Datasets/I94_SAS_Label_Descriptors.json'
    LabelDescriptorToJson(inpt_file,labels,opt_file)
"""
