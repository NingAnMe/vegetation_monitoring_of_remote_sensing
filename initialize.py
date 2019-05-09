#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/9
@Author  : AnNing
"""
import yaml


def load_yaml_file(in_file):
    with open(in_file, 'r') as stream:
        config_data = yaml.load(stream)
    return config_data
