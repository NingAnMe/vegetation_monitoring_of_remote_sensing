#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/1/15
@Author  : AnNing
"""
import h5py
import numpy as np


def write_hdf5_and_compress(out_file, data_out):
    """
    :param out_file: (str)
    :param data_out: (dict)
    :return:
    """
    if not data_out:
        return
    compression = 'gzip'
    compression_opts = 5
    shuffle = True
    with h5py.File(out_file, 'w') as hdf5:
        for key in data_out:
            if isinstance(data_out[key], dict):
                group_name = key
                group_data = data_out[key]
                if isinstance(group_data, dict):
                    for dataset_name in group_data:
                        data = group_data[dataset_name]
                        # 处理
                        hdf5.create_dataset('/{}/{}'.format(group_name, dataset_name),
                                            dtype=np.float32, data=data, compression=compression,
                                            compression_opts=compression_opts,
                                            shuffle=shuffle)
            else:
                dataset_name = key
                data = data_out[dataset_name]
                # 处理
                hdf5.create_dataset(dataset_name, data=data, compression=compression,
                                    compression_opts=compression_opts,
                                    shuffle=shuffle)
    print('>>> {}'.format(out_file))
